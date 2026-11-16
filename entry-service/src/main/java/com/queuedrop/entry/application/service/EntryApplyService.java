package com.queuedrop.entry.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.EventPublishException;
import com.queuedrop.entry.application.port.GateResult;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 응모: Redis 판정 → entry.created 동기 발행 → (발행 실패 시) 판정 보상. DB 를 쓰지 않는다 (D1). */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntryApplyService {

    private final EntryGate entryGate;
    private final EntryEventPublisher eventPublisher;
    private final Clock clock;

    public EntryApplyResult apply(EntryApplyCommand command) {
        Instant now = clock.instant();

        GateResult gate = entryGate.tryApply(command.dropId(), command.userId(), now);
        if (!gate.isOk()) {
            throw new ApplicationException(EntryErrorCode.from(gate.outcome()));
        }

        EntryCreatedPayload payload = new EntryCreatedPayload(
                UUID.randomUUID().toString(), command.dropId(), command.userId(),
                gate.entrySeq(), now);

        try {
            eventPublisher.publishCreated(payload);
        } catch (EventPublishException e) {
            compensateQuietly(command);
            throw new ApplicationException(EntryErrorCode.TEMPORARILY_FAILED, e);
        }

        return new EntryApplyResult(gate.entrySeq());
    }

    private void compensateQuietly(EntryApplyCommand command) {
        try {
            entryGate.compensate(command.dropId(), command.userId());
        } catch (RuntimeException e) {
            // 보상 실패: 정원 한 자리가 새는 쪽(언더 응모)이라 정합성 위반은 아니다 (D2). 알림 대상.
            log.error("보상 실패 dropId={} userId={}", command.dropId(), command.userId(), e);
        }
    }
}
