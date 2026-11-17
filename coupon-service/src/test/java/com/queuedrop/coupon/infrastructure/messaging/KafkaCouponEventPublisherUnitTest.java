package com.queuedrop.coupon.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
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
 * 어댑터의 예외 변환 단위 테스트 (T6-12 보강, 3.3 "RuntimeException 도 EventPublishException").
 * 포트 밖으로 Kafka 예외가 새면 CouponIssueService 의 보상이 실행되지 않는다 (500 + Redis 에만 발급).
 * 애플리케이션 재시도가 없는지(send 1회)도 확인한다. 브로커가 필요 없어 KafkaTemplate 은 mock.
 */
class KafkaCouponEventPublisherUnitTest {

    private static final CouponIssuedPayload PAYLOAD = new CouponIssuedPayload(
            "req-1", 42L, 1001L, Instant.parse("2026-11-20T10:30:00Z"));

    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaCouponEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new KafkaCouponEventPublisher(kafkaTemplate, Duration.ofMillis(200));
    }

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    @DisplayName("[T6-12] 성공하면 예외 없이 반환하고, coupon.issued 에 key={couponEventId}:{userId}, CouponIssued 를 1회 보낸다")
    void successSendsOnce() {
        // Given
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // When
        publisher.publishIssued(PAYLOAD);

        // Then
        verify(kafkaTemplate, times(1)).send(eq("coupon.issued"), eq("42:1001"),
                eq(new CouponIssued("req-1", 42L, 1001L, Instant.parse("2026-11-20T10:30:00Z"))));
    }

    @Test
    @DisplayName("[T6-12] send() 가 동기로 직렬화 실패를 던지면 EventPublishException 으로 감싼다")
    void syncSerializationFailureIsWrapped() {
        // Given
        SerializationException cause = new SerializationException("직렬화 실패");
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(cause);

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCause(cause);
    }

    @Test
    @DisplayName("[T6-12] send() 가 동기로 Spring KafkaException(max.block.ms 초과 등)을 던지면 EventPublishException 으로 감싼다")
    void syncSpringKafkaExceptionIsWrapped() {
        // Given
        KafkaException cause = new KafkaException("Send failed");
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(cause);

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCause(cause);
    }

    @Test
    @DisplayName("[T6-12] send() 가 동기로 임의의 RuntimeException 을 던져도 EventPublishException 으로 감싼다")
    void syncAnyRuntimeExceptionIsWrapped() {
        // Given
        IllegalStateException cause = new IllegalStateException("producer closed");
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(cause);

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCause(cause);
    }

    @Test
    @DisplayName("[T6-12] future 가 예외로 완료되면(ExecutionException) EventPublishException 으로 감싸고 다시 send 하지 않는다")
    void futureFailureIsWrappedWithoutRetry() {
        // Given
        CompletableFuture<SendResult<String, Object>> failed =
                CompletableFuture.failedFuture(new IllegalStateException("브로커 기록 실패"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("[T6-12] future 가 await-timeout 안에 완료되지 않으면 EventPublishException(TimeoutException) 을 던진다")
    void futureTimeoutIsWrapped() {
        // Given: 끝나지 않는 future, 대기 200ms
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("[T6-12] 대기 중 인터럽트되면 EventPublishException 을 던지고 인터럽트 상태를 복원한다")
    void interruptIsWrappedAndFlagRestored() {
        // Given: 끝나지 않는 future + 이미 인터럽트된 스레드 → get 이 즉시 InterruptedException
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        // When / Then
        assertThatThrownBy(() -> publisher.publishIssued(PAYLOAD))
                .isInstanceOf(EventPublishException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }
}
