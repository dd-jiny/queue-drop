package com.queuedrop.coupon.scenario;

import static com.queuedrop.coupon.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.support.KafkaConsumption;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 쿠폰 시나리오 테스트 공통 설정. 모든 시나리오 클래스가 상속해 같은 컨텍스트를 쓴다
 * (@AutoConfigureMockMvc 도 공통으로 둬서 컨텍스트 캐시 키를 맞춘다). MyCouponQueryTest 는 가변 Clock 이 필요해 따로 둔다.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        CouponScenarioTestSupport.Config.class})
abstract class CouponScenarioTestBase {

    /** 비동기 저장(coupon.issued 소비) 대기 상한 */
    static final Duration CONSUME_WAIT = Duration.ofSeconds(30);

    @Autowired StringRedisTemplate redis;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired CouponGate gate;
    @Autowired CouponScenarioTestSupport.ControllableCouponEventPublisher publisher;
    @Autowired CouponScenarioTestSupport.ProducerWarmUp producerWarmUp;

    @BeforeEach
    void resetState() throws Exception {
        publisher.reset();
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE issued_coupons, coupon_events RESTART IDENTITY");
        warmUpProducer();
    }

    /**
     * NewTopic 직후 메타데이터 전파 대기(원본·DLT) + 첫 send 를 미리 한 번 보낸다 (동시성 테스트의 첫 발행이
     * max.block.ms 1초에 걸리지 않게). couponEventId 0 은 Redis 에 발급 기록이 없어 저장 리스너가 claim false 로 무시한다.
     */
    private void warmUpProducer() throws Exception {
        if (producerWarmUp.isDone()) {
            return;
        }
        awaitTopicMetadata(kafkaTemplate, Topics.COUPON_ISSUED, Topics.COUPON_ISSUED_DLT);
        kafkaTemplate.send(Topics.COUPON_ISSUED, "0:0",
                        new CouponIssued(UUID.randomUUID().toString(), 0L, 0L, CouponScenarioTestSupport.NOW))
                .get(10, TimeUnit.SECONDS);
        producerWarmUp.markDone();
    }

    /** 발급 중인 쿠폰 이벤트: DB 행(FK 대상) + Redis 초기화 (등록 유스케이스와 같은 결과). 반환값은 couponEventId */
    long givenOpenEvent(int totalQuantity) {
        Long id = jdbc.queryForObject("""
                INSERT INTO coupon_events (name, total_quantity, issue_start_at, created_at)
                VALUES (?, ?, ?, ?) RETURNING id""", Long.class,
                "시나리오 쿠폰", totalQuantity,
                Timestamp.from(CouponScenarioTestSupport.START_AT),
                Timestamp.from(CouponScenarioTestSupport.START_AT.minusSeconds(3600)));
        gate.initialize(id, totalQuantity, CouponScenarioTestSupport.START_AT);
        return id;
    }

    String remaining(long couponEventId) {
        return redis.opsForValue().get("coupon:" + couponEventId + ":remaining");
    }

    long issuedCount(long couponEventId) {
        return redis.opsForSet().size("coupon:" + couponEventId + ":issued");
    }

    boolean isIssued(long couponEventId, long userId) {
        return Boolean.TRUE.equals(
                redis.opsForSet().isMember("coupon:" + couponEventId + ":issued", String.valueOf(userId)));
    }

    Object requestOf(long couponEventId, long userId) {
        return redis.opsForHash().get("coupon:" + couponEventId + ":requests", String.valueOf(userId));
    }

    boolean isPersisted(long couponEventId, long userId) {
        return Boolean.TRUE.equals(
                redis.opsForSet().isMember("coupon:" + couponEventId + ":persisted", String.valueOf(userId)));
    }

    long dbCount(long couponEventId) {
        return jdbc.queryForObject("SELECT count(*) FROM issued_coupons WHERE coupon_event_id = ?",
                Long.class, couponEventId);
    }

    long dbCount(long couponEventId, long userId) {
        return jdbc.queryForObject("SELECT count(*) FROM issued_coupons WHERE coupon_event_id = ? AND user_id = ?",
                Long.class, couponEventId, userId);
    }

    /** coupon.issued 를 모두 소비할 때까지 기다린다 (늦게 처리되는 유령 이벤트까지 반영된 뒤 단언하려고) */
    void awaitAllIssuedEventsConsumed() {
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200))
                .until(() -> KafkaConsumption.fullyConsumed(kafkaAdmin, Topics.COUPON_ISSUED));
    }
}
