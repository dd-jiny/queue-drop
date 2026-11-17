package com.queuedrop.coupon.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.dto.CouponIssueResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.CouponEventPublisher;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
import com.queuedrop.coupon.application.port.GateResult;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 발급: 요청 ID 생성 → Redis 판정 → coupon.issued 동기 발행 → (실패 시) 같은 요청 ID 로 보상.
 * DB 를 쓰지 않는다 (4단계 EntryApplyService 와 같은 흐름, E-40·E-41).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CouponIssueService {

    private final CouponGate gate;
    private final CouponEventPublisher publisher;
    private final Clock clock;

    public CouponIssueResult issue(CouponIssueCommand command) {
        Instant now = clock.instant();
        String requestId = UUID.randomUUID().toString();    // 요청 ID = 메시지 eventId. 판정 전에 만든다 (E-40)

        GateResult result;
        try {
            result = gate.tryIssue(command.couponEventId(), command.userId(), requestId, now);
        } catch (GateUnavailableException e) {
            // Lua 실행 여부를 모름 (E-41): 실행됐다면 되돌리고(COMPENSATED), 아니면 NOT_OURS 로 무변화
            compensateQuietly(command, requestId);
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
        if (result.outcome() != IssueOutcome.OK) {
            throw new ApplicationException(CouponErrorCode.from(result.outcome()));
        }

        CouponIssuedPayload payload = new CouponIssuedPayload(
                requestId, command.couponEventId(), command.userId(), now);
        try {
            publisher.publishIssued(payload);
        } catch (EventPublishException e) {
            Optional<CompensationResult> compensation = compensateQuietly(command, requestId);
            if (compensation.filter(r -> r == CompensationResult.ALREADY_PERSISTED).isPresent()) {
                // 컨슈머가 이미 저장 확정 = 실제로 발급됨 → 성공 (불변식 "성공 = 기록")
                log.warn("발행 실패로 판단했으나 이미 저장됨 couponEventId={} userId={} requestId={}",
                        command.couponEventId(), command.userId(), requestId);
                return new CouponIssueResult(command.couponEventId(), now);
            }
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
        return new CouponIssueResult(command.couponEventId(), now);
    }

    /** @return 보상 결과. 보상 자체가 실패하면 empty (로그만 남기고 호출자는 503) */
    private Optional<CompensationResult> compensateQuietly(CouponIssueCommand command, String requestId) {
        try {
            return Optional.of(gate.compensate(command.couponEventId(), command.userId(), requestId));
        } catch (RuntimeException e) {
            // 수량 한 자리가 새는 쪽(언더 발급)이라 초과 발급은 아니다. 알림 대상.
            log.error("보상 실패 couponEventId={} userId={} requestId={}",
                    command.couponEventId(), command.userId(), requestId, e);
            return Optional.empty();
        }
    }
}
