package com.queuedrop.entry.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.queuedrop.common.event.DrawCompleted;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.port.EventPublishException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * draw.completed 어댑터의 매핑·예외 변환 단위 테스트 (T5-20 보강).
 * 포트 밖으로 Kafka 예외가 새도 DrawService.publishSafely 가 잡지만, 포트 계약(실패 시 EventPublishException)을 고정한다.
 * 브로커가 필요 없어 KafkaTemplate 은 mock.
 */
class KafkaDrawEventPublisherUnitTest {

    private static final Instant DRAWN_AT = Instant.parse("2026-11-20T11:00:05Z");
    private static final DrawCompletedPayload PAYLOAD = new DrawCompletedPayload(42L, 120, 10, DRAWN_AT);

    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaDrawEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new KafkaDrawEventPublisher(kafkaTemplate, Duration.ofMillis(200));
    }

    @Test
    @DisplayName("[T5-20] (경계) future 가 await-timeout(200ms) 안에 완료되지 않으면 EventPublishException(원인 TimeoutException) 을 던진다")
    void futureTimeoutIsWrapped() {
        // Given — 끝나지 않는 future
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());

        // When
        long start = System.nanoTime();
        Throwable thrown = catchThrowable(() -> publisher.publishCompleted(PAYLOAD));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // Then — 주입한 대기 시간을 따른다 (상수 3초가 아님)
        assertThat(thrown).isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("[T5-20] 토픽 draw.completed, 키 = dropId 문자열, 값 = 필드가 같은 common DrawCompleted 로 보낸다")
    void mapsTopicKeyAndMessage() {
        // Given
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // When
        publisher.publishCompleted(PAYLOAD);

        // Then
        verify(kafkaTemplate).send(eq("draw.completed"), eq("42"), eq(new DrawCompleted(42L, 120, 10, DRAWN_AT)));
    }

    @Test
    @DisplayName("[T5-20] (경계) send() 가 동기로 RuntimeException(직렬화 실패)을 던지면 EventPublishException 으로 감싼다")
    void syncSerializationFailureIsWrapped() {
        // Given
        SerializationException serialization = new SerializationException("직렬화 실패");
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(serialization);

        // When / Then
        assertThatThrownBy(() -> publisher.publishCompleted(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasRootCause(serialization);
    }

    @Test
    @DisplayName("[T5-20] (경계) send() 가 동기로 Spring KafkaException(max.block.ms 초과 등)을 던지면 EventPublishException 으로 감싼다")
    void syncSpringKafkaExceptionIsWrapped() {
        // Given
        KafkaException kafka = new KafkaException("Send failed");
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(kafka);

        // When / Then
        assertThatThrownBy(() -> publisher.publishCompleted(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasRootCause(kafka);
    }

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    @DisplayName("[T5-20] (경계) 대기 중 인터럽트되면 EventPublishException 을 던지고 인터럽트 상태를 복원한다 (스케줄러 종료 시)")
    void interruptIsWrappedAndFlagRestored() {
        // Given — 끝나지 않는 future + 이미 인터럽트된 스레드 → get 이 즉시 InterruptedException
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        // When / Then
        assertThatThrownBy(() -> publisher.publishCompleted(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    @DisplayName("[T5-20] (경계) future 가 예외로 완료되면 EventPublishException 으로 감싼다")
    void futureFailureIsWrapped() {
        // Given
        CompletableFuture<SendResult<String, Object>> failed =
                CompletableFuture.failedFuture(new IllegalStateException("브로커 기록 실패"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        // When / Then
        assertThatThrownBy(() -> publisher.publishCompleted(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }
}
