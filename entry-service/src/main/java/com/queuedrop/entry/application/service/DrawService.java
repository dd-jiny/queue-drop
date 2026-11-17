package com.queuedrop.entry.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.config.DrawProperties;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.dto.DrawOutcome;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.dto.ManualDrawCommand;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.application.port.ApplicantCounter;
import com.queuedrop.entry.application.port.DrawEventPublisher;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import com.queuedrop.entry.domain.repository.EntryRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 마감·추첨 조율. 트랜잭션을 직접 열지 않고 추첨 트랜잭션은 DrawTransaction 에 위임한다.
 * 건수 비교(Redis)와 발행(Kafka)은 트랜잭션 밖에서 한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DrawService {

    private final DropSnapshotRepository snapshots;
    private final EntryRepository entries;
    private final DrawResultRepository results;
    private final ApplicantCounter applicantCounter;
    private final EntryGate gate;
    private final DrawTransaction drawTransaction;
    private final DrawEventPublisher publisher;
    private final DrawProperties properties;

    /** 1) 마감 잠금: OPEN → CLOSING, Redis state=CLOSED */
    public void closeDueDrops(Instant now) {
        for (Long dropId : snapshots.findDueOpenIds(now)) {
            try {
                if (snapshots.markClosingIfOpen(dropId, now)) {   // 1곳만 true
                    closeGateQuietly(dropId);
                    log.info("drop closed dropId={}", dropId);
                }
            } catch (RuntimeException e) {                        // 드로우 단위 격리: 다음 드로우는 계속
                log.error("drop close failed dropId={}", dropId, e);
            }
        }
    }

    /** 실패해도 응모 차단은 Lua 의 closeAt 검사가 보장한다. 재시도하지 않는다 */
    private void closeGateQuietly(Long dropId) {
        try {
            gate.close(dropId);
        } catch (GateUnavailableException e) {
            log.warn("entry gate close failed dropId={}", dropId, e);
        }
    }

    /** 2) CLOSING 드로우마다 건수 비교 후 추첨 */
    public void drawReadyDrops(Instant now) {
        for (DropSnapshot snapshot : snapshots.findClosing()) {
            try {
                drawIfReady(snapshot, now);
            } catch (RuntimeException e) {     // DB 예외 등. 롤백되어 CLOSING 유지 → 다음 주기 재시도
                log.error("draw failed dropId={}", snapshot.getDropId(), e);
            }
        }
    }

    private void drawIfReady(DropSnapshot snapshot, Instant now) {
        Long dropId = snapshot.getDropId();
        long redisCount;
        try {
            redisCount = applicantCounter.count(dropId);
        } catch (GateUnavailableException e) {
            log.warn("applicant count failed dropId={}", dropId, e);   // 이 드로우만 건너뜀. 다음 주기 재시도
            return;
        }
        long dbCount = entries.countByDropId(dropId);

        if (redisCount != dbCount) {
            if (snapshot.isClosingTooLong(now, properties.closingWarnThreshold())) {
                log.warn("draw waiting too long dropId={} redis={} db={}", dropId, redisCount, dbCount);
            }
            return;                                          // 다음 주기 재확인
        }

        DrawOutcome outcome = drawTransaction.execute(dropId, now);   // 커밋까지 완료
        if (outcome.drawn()) {
            afterCommit(outcome.payload());
        }
    }

    /** 관리자 수동 추첨 (D5-1): 건수 비교 없이 DB 응모 기준으로 추첨. Redis 에만 있는 응모는 빠진다 */
    public DrawSummaryResult drawManually(ManualDrawCommand command, Instant now) {
        AdminGuard.requireAdmin(command.requester());          // 403 FORBIDDEN (존재 여부보다 먼저)
        Long dropId = command.dropId();
        DropSnapshot snapshot = snapshots.findById(dropId)
                .orElseThrow(() -> new ApplicationException(DrawErrorCode.DROP_NOT_FOUND));
        if (snapshot.isDrawn()) throw new ApplicationException(DrawErrorCode.ALREADY_DRAWN);
        if (!snapshot.isClosing()) throw new ApplicationException(DrawErrorCode.NOT_CLOSING);   // OPEN: 마감 전

        long redisCount = safeCount(dropId);                    // 감사 로그용. Redis 장애여도 수동 추첨은 진행
        long dbCount = entries.countByDropId(dropId);
        log.warn("manual draw dropId={} by userId={} redis={} db={}",
                dropId, command.requester().userId(), redisCount, dbCount);

        DrawOutcome outcome = drawTransaction.execute(dropId, now);
        if (!outcome.drawn()) throw new ApplicationException(DrawErrorCode.ALREADY_DRAWN);  // 스케줄러와 경합해 짐
        afterCommit(outcome.payload());
        return results.findByDropId(dropId).map(DrawSummaryResult::from).orElseThrow();
    }

    /** @return Redis 응모자 수. 장애면 -1 (로그용) */
    private long safeCount(Long dropId) {
        try {
            return applicantCounter.count(dropId);
        } catch (GateUnavailableException e) {
            return -1;
        }
    }

    /** 커밋 후: 발행(실패 시 재발행 대상) → 응모 키 만료 (E-42). 둘 다 실패해도 추첨 결과는 확정 */
    private void afterCommit(DrawCompletedPayload payload) {
        publishSafely(payload);
        try {
            gate.expireAfterDraw(payload.dropId(), properties.keyTtlAfterDraw());
        } catch (GateUnavailableException e) {
            log.warn("entry keys expire failed dropId={}", payload.dropId(), e);   // 키만 남음. 정합성 영향 없음
        }
    }

    /** 3) 발행 실패분 재발행 */
    public void republishUnpublished() {
        for (DrawResult r : results.findUnpublished()) {
            publishSafely(new DrawCompletedPayload(r.getDropId(), r.getApplicantCount(), r.getWinnerCount(),
                    r.getDrawnAt()));
        }
    }

    private void publishSafely(DrawCompletedPayload payload) {
        try {
            publisher.publishCompleted(payload);
        } catch (RuntimeException e) {
            log.warn("draw.completed publish failed dropId={}", payload.dropId(), e);   // 다음 주기 재발행
            return;
        }
        try {
            results.markPublished(payload.dropId());
        } catch (RuntimeException e) {
            log.warn("draw.completed published but mark failed dropId={}", payload.dropId(), e);  // 다음 주기 중복 발행 (소비자 멱등)
        }
    }
}
