package com.queuedrop.entry.infrastructure.messaging;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** EntryEventPublisher 구현. application DTO 를 common 메시지로 바꿔 브로커 기록 확인까지 기다린다. */
@Component
public class KafkaEntryEventPublisher implements EntryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Duration awaitTimeout;

    KafkaEntryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                             @Value("${entry.publish.await-timeout}") Duration awaitTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.awaitTimeout = awaitTimeout;
    }

    @Override
    public void publishCreated(EntryCreatedPayload p) {
        EntryCreated message = new EntryCreated(
                p.eventId(), p.dropId(), p.userId(), p.entrySeq(), p.appliedAt());
        String key = p.dropId() + ":" + p.userId();         // 파티션 키 {dropId}:{userId}

        try {
            kafkaTemplate.send(Topics.ENTRY_CREATED, key, message)
                    .get(awaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("발행 대기 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("entry.created 발행 실패", e);
        } catch (RuntimeException e) {          // send() 가 동기로 던지는 경우 (직렬화 실패, max.block.ms 초과 등)
            throw new EventPublishException("entry.created 발행 실패", e);
        }
    }
}
