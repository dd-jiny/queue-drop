package com.queuedrop.entry.infrastructure.messaging;

import com.queuedrop.common.event.DrawCompleted;
import com.queuedrop.common.event.Topics;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.port.DrawEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** DrawEventPublisher 구현. application DTO 를 common 메시지로 바꿔 브로커 기록 확인까지 기다린다 */
@Component
public class KafkaDrawEventPublisher implements DrawEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Duration awaitTimeout;

    KafkaDrawEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                            @Value("${entry.publish.await-timeout}") Duration awaitTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.awaitTimeout = awaitTimeout;
    }

    @Override
    public void publishCompleted(DrawCompletedPayload p) {
        DrawCompleted message = new DrawCompleted(p.dropId(), p.applicantCount(), p.winnerCount(), p.drawnAt());
        String key = String.valueOf(p.dropId());            // 파티션 키 dropId

        try {
            kafkaTemplate.send(Topics.DRAW_COMPLETED, key, message)
                    .get(awaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("발행 대기 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("draw.completed 발행 실패", e);
        } catch (RuntimeException e) {          // send() 가 동기로 던지는 경우 (직렬화 실패, max.block.ms 초과 등)
            throw new EventPublishException("draw.completed 발행 실패", e);
        }
    }
}
