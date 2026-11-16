package com.queuedrop.product.infrastructure.messaging;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.product.application.port.DropCreatedPayload;
import com.queuedrop.product.application.port.DropEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 커밋 후 발행 (1단계 KafkaUserEventPublisher 와 같은 패턴). 트랜잭션 밖(재발행)이면 즉시 발행한다.
 * 재시도는 producer 내부 재시도(멱등, delivery.timeout.ms 안)에 맡기고, 최종 실패는 ERROR 로그 → 관리자 재발행.
 */
@Slf4j
@Component
@RequiredArgsConstructor
class KafkaDropEventPublisher implements DropEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publishCreated(DropCreatedPayload p) {
        DropCreated message = new DropCreated(p.dropId(), p.productId(), p.title(),
                p.entryCapacity(), p.winnerCount(), p.openAt(), p.closeAt());
        Runnable send = () -> {
            try {
                kafkaTemplate.send(Topics.DROP_CREATED, String.valueOf(p.dropId()), message)
                        .whenComplete((r, ex) -> {
                            if (ex != null) logFailure(p.dropId(), ex);
                        });
            } catch (RuntimeException ex) {   // max.block.ms 초과 등 send() 자체 실패 — 커밋된 등록을 되돌리지 않는다
                logFailure(p.dropId(), ex);
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

    private void logFailure(Long dropId, Throwable ex) {
        log.error("drop.created 최종 발행 실패 — 관리자 재발행 필요: dropId={}", dropId, ex);
    }
}
