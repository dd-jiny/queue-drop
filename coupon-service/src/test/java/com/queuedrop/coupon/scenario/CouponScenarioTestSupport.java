package com.queuedrop.coupon.scenario;

import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.Topics;
import com.queuedrop.coupon.application.port.CouponEventPublisher;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 6단계 시나리오 테스트 공통 지원 (단계 문서 4장 T6-13 ~ T6-16, T6-22, T6-27).
 * 세 시나리오 클래스가 같은 @Import 조합을 써서 컨텍스트(와 컨테이너)를 공유한다.
 */
final class CouponScenarioTestSupport {

    /** 고정 판정 시각. 쿠폰 이벤트는 START_AT 에 시작한 것으로 초기화한다 */
    static final Instant NOW = Instant.parse("2026-11-20T10:30:00Z");
    static final Instant START_AT = Instant.parse("2026-11-20T10:00:00Z");

    private CouponScenarioTestSupport() {}

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {
        /** 운영과 같은 파티션 수 (coupon.issued 6). DLT 는 원본 파티션 번호로 보내므로 같은 수 */
        @Bean NewTopic couponIssuedTopic()    { return TopicBuilder.name(Topics.COUPON_ISSUED).partitions(6).replicas(1).build(); }
        @Bean NewTopic couponIssuedDltTopic() { return TopicBuilder.name(Topics.COUPON_ISSUED_DLT).partitions(6).replicas(1).build(); }

        /** producer 예열 여부. 컨텍스트(= Kafka 컨테이너)마다 한 번 — static 이면 다른 컨텍스트에서 예열이 빠진다 */
        @Bean
        ProducerWarmUp producerWarmUp() {
            return new ProducerWarmUp();
        }

        @Bean @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        /** 실제 Kafka 어댑터(빈 이름 kafkaCouponEventPublisher, package-private 클래스라 포트 타입으로 받는다)에 위임 */
        @Bean @Primary
        ControllableCouponEventPublisher controllableCouponEventPublisher(
                @Qualifier("kafkaCouponEventPublisher") CouponEventPublisher delegate, JdbcTemplate jdbc) {
            return new ControllableCouponEventPublisher(delegate, jdbc);
        }
    }

    static final class ProducerWarmUp {
        private final AtomicBoolean done = new AtomicBoolean();

        boolean isDone() { return done.get(); }
        void markDone()  { done.set(true); }
    }

    /**
     * 테스트 전용 Publisher. 기본은 실제 KafkaCouponEventPublisher 로 위임한다.
     * - alwaysFail: 보내지 않고 항상 실패 (T6-15)
     * - loseAckOnceFor: 실제로 보낸 뒤 실패 (ack 유실 → 유령 이벤트, 보상이 먼저 T6-16·T6-22)
     * - consumeThenFailOnceFor: 실제로 보내고 저장 컨슈머가 DB 에 저장할 때까지 기다린 뒤 실패 (소비가 먼저 T6-27)
     * 브로커에 기록된 발행의 요청 ID(eventId)를 사용자별로 남긴다.
     */
    static final class ControllableCouponEventPublisher implements CouponEventPublisher {

        private final CouponEventPublisher delegate;
        private final JdbcTemplate jdbc;
        private volatile boolean alwaysFail;
        private final Set<Long> ackLostOnce = ConcurrentHashMap.newKeySet();
        private final Set<Long> consumedThenFailOnce = ConcurrentHashMap.newKeySet();
        private final Map<Long, List<String>> sentRequestIds = new ConcurrentHashMap<>();

        ControllableCouponEventPublisher(CouponEventPublisher delegate, JdbcTemplate jdbc) {
            this.delegate = delegate;
            this.jdbc = jdbc;
        }

        void alwaysFail()                        { alwaysFail = true; }
        void loseAckOnceFor(long userId)         { ackLostOnce.add(userId); }
        void consumeThenFailOnceFor(long userId) { consumedThenFailOnce.add(userId); }

        /** 브로커에 기록된 이 사용자의 발행 요청 ID (발행 순서) */
        List<String> sentRequestIds(long userId) {
            return List.copyOf(sentRequestIds.getOrDefault(userId, List.of()));
        }

        void reset() {
            alwaysFail = false;
            ackLostOnce.clear();
            consumedThenFailOnce.clear();
            sentRequestIds.clear();
        }

        @Override
        public void publishIssued(CouponIssuedPayload payload) {
            if (alwaysFail) {
                throw new EventPublishException("발행 실패 시뮬레이션", null);
            }
            delegate.publishIssued(payload);                          // 브로커에는 기록됨
            sentRequestIds.computeIfAbsent(payload.userId(), k -> new CopyOnWriteArrayList<>()).add(payload.eventId());
            if (ackLostOnce.remove(payload.userId())) {
                throw new EventPublishException("ack 유실 시뮬레이션", null);
            }
            if (consumedThenFailOnce.remove(payload.userId())) {
                // claim 은 INSERT 전에 실행되므로 "DB 에 행이 있음" 이면 claim 이 보상보다 먼저임이 보장된다
                await().atMost(CouponScenarioTestBase.CONSUME_WAIT).pollInterval(Duration.ofMillis(50))
                        .until(() -> persisted(payload.couponEventId(), payload.userId()));
                throw new EventPublishException("ack 유실 시뮬레이션 (컨슈머가 먼저 처리)", null);
            }
        }

        private boolean persisted(long couponEventId, long userId) {
            Long count = jdbc.queryForObject(
                    "SELECT count(*) FROM issued_coupons WHERE coupon_event_id = ? AND user_id = ?",
                    Long.class, couponEventId, userId);
            return count != null && count > 0;
        }
    }
}
