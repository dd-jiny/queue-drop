package com.queuedrop.user.infrastructure.messaging;

import com.queuedrop.common.event.Topics;
import com.queuedrop.common.event.UserRegistered;
import com.queuedrop.user.application.port.UserEventPublisher;
import com.queuedrop.user.application.port.UserRegisteredPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 커밋 후 발행. application 은 "발행한다"만 알고 시점은 이 어댑터가 책임진다.
 * 회원가입 이벤트는 비동기 발행(응답 대기 X) — 실패하면 랭킹 닉네임만 누락되므로 로그로 남긴다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
class KafkaUserEventPublisher implements UserEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publishRegistered(UserRegisteredPayload payload) {
        UserRegistered message = new UserRegistered(payload.userId(), payload.nickname());
        Runnable send = () -> {
            try {
                kafkaTemplate.send(Topics.USER_REGISTERED, String.valueOf(payload.userId()), message)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.warn("user.registered publish failed: userId={}", payload.userId(), ex);
                        });
            } catch (RuntimeException ex) {   // max.block.ms 초과 등 send() 자체 실패 — 커밋된 가입을 되돌리지 않는다
                log.warn("user.registered publish failed: userId={}", payload.userId(), ex);
            }
        };

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send.run();
                }
            });
        } else {
            send.run();
        }
    }
}
