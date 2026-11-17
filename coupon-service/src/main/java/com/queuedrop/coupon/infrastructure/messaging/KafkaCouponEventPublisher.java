package com.queuedrop.coupon.infrastructure.messaging;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.coupon.application.port.CouponEventPublisher;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** CouponEventPublisher 구현. application DTO 를 common 메시지로 바꿔 브로커 기록 확인까지 기다린다. */
@Component
class KafkaCouponEventPublisher implements CouponEventPublisher {

    private final KafkaTemplate<String, Object> kafka;
    private final Duration awaitTimeout;

    KafkaCouponEventPublisher(KafkaTemplate<String, Object> kafka,
                              @Value("${coupon.publish.await-timeout}") Duration awaitTimeout) {
        this.kafka = kafka;
        this.awaitTimeout = awaitTimeout;
    }

    @Override
    public void publishIssued(CouponIssuedPayload p) {
        CouponIssued message = new CouponIssued(p.eventId(), p.couponEventId(), p.userId(), p.issuedAt());
        String key = p.couponEventId() + ":" + p.userId();      // 파티션 키 {couponEventId}:{userId}
        try {
            kafka.send(Topics.COUPON_ISSUED, key, message)
                    .get(awaitTimeout.toMillis(), TimeUnit.MILLISECONDS);   // 애플리케이션 재시도 없음
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("발행 대기 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("coupon.issued 발행 실패", e);
        } catch (RuntimeException e) {          // send() 가 동기로 던지는 경우 (직렬화 실패, max.block.ms 초과 등)
            throw new EventPublishException("coupon.issued 발행 실패", e);   // 보상 경로로 보낸다
        }
    }
}
