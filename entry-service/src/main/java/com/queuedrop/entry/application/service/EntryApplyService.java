package com.queuedrop.entry.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.EventPublishException;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 응모: 요청 ID(eventId) 생성 → Redis 판정 → entry.created 동기 발행 → (실패 시) 같은 eventId 로 보상.
 * DB 를 쓰지 않는다 (D1).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntryApplyService {

    private final EntryGate entryGate;
    private final EntryEventPublisher eventPublisher;
    private final Clock clock;

    public EntryApplyResult apply(EntryApplyCommand command) {
        Instant now = clock.instant();
        String eventId = UUID.randomUUID().toString();      // 요청 ID = eventId. 판정 전에 만든다 (E-40)

        GateResult gate;
        try {
            gate = entryGate.tryApply(command.dropId(), command.userId(), eventId, now);
        } catch (GateUnavailableException e) {
            // Lua 실행 여부를 모름 (E-41): 실행됐다면 되돌리고(COMPENSATED), 아니면 NOT_OURS 로 무변화
            compensateQuietly(command, eventId);
            throw new ApplicationException(EntryErrorCode.TEMPORARILY_FAILED, e);
        }
        if (!gate.isOk()) {
            throw new ApplicationException(EntryErrorCode.from(gate.outcome()));
        }

        EntryCreatedPayload payload = new EntryCreatedPayload(
                eventId, command.dropId(), command.userId(), gate.entrySeq(), now);

        try {
            eventPublisher.publishCreated(payload);
        } catch (EventPublishException e) {
            Optional<CompensationResult> result = compensateQuietly(command, eventId);
            if (result.filter(r -> r == CompensationResult.ALREADY_PERSISTED).isPresent()) {
                // 컨슈머가 이미 저장 확정 = 실제로 기록됨. 불변식("성공 = 기록")과 맞으므로 성공 (E-40)
                log.warn("발행 실패로 판단했으나 이미 저장됨 dropId={} userId={} eventId={}",
                        command.dropId(), command.userId(), eventId);
                return new EntryApplyResult(gate.entrySeq());
            }
            throw new ApplicationException(EntryErrorCode.TEMPORARILY_FAILED, e);
        }

        return new EntryApplyResult(gate.entrySeq());
    }

    /** @return 보상 결과. 보상 자체가 실패하면 empty (로그만 남기고 호출자는 503) */
    private Optional<CompensationResult> compensateQuietly(EntryApplyCommand command, String eventId) {
        try {
            return Optional.of(entryGate.compensate(command.dropId(), command.userId(), eventId));
        } catch (RuntimeException e) {
            // 보상 실패: 정원 한 자리가 새는 쪽(언더 응모)이라 정원 초과는 아니다 (D2). 알림 대상.
            // tryApply 타임아웃 경로에서는 Redis 에만 응모가 남아 D6 불일치가 된다 (단계 문서 6.3 리스크)
            log.error("보상 실패 dropId={} userId={} eventId={}",
                    command.dropId(), command.userId(), eventId, e);
            return Optional.empty();
        }
    }
}
