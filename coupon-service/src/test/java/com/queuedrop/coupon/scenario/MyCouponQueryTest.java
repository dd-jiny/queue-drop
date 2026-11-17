package com.queuedrop.coupon.scenario;

import static com.queuedrop.coupon.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * 내 쿠폰 조회 전체 컨텍스트 (단계 문서 4장 T6-20, REQ-COUPON-04).
 * 등록(API) → Redis 초기화 → 발급(API) → coupon.issued → 저장 리스너 → GET /me 까지 실제 경로로 확인한다.
 * 시각은 @Primary 가변 Clock 으로 제어한다. @Future 검증은 시스템 시각 기준이라 시작 시각은 실제 시각 + 1일.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        MyCouponQueryTest.Config.class})
class MyCouponQueryTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration CONSUME_WAIT = Duration.ofSeconds(30);

    /** 테스트가 시각을 옮길 수 있는 Clock */
    static final class MutableClock extends Clock {
        private volatile Instant instant = Instant.EPOCH;

        void set(Instant instant) {
            this.instant = instant;
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {
        @Bean @Primary
        MutableClock testClock() {
            return new MutableClock();
        }

        @Bean
        NewTopic couponIssuedTopic() {
            return TopicBuilder.name(Topics.COUPON_ISSUED).partitions(1).replicas(1).build();
        }
    }

    @Autowired MockMvc mockMvc;
    @Autowired MutableClock clock;
    @Autowired JdbcTemplate jdbc;
    @Autowired StringRedisTemplate redis;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void reset() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE issued_coupons, coupon_events RESTART IDENTITY");
        awaitTopicMetadata(kafkaTemplate, Topics.COUPON_ISSUED);   // 첫 발행이 max.block.ms(1초)에 걸리지 않게
    }

    private long registerEvent(String name, Instant issueStartAt) throws Exception {
        String body = """
                {"name":"%s","totalQuantity":10,"issueStartAt":"%s"}""".formatted(name, issueStartAt);
        String response = mockMvc.perform(post("/api/coupons/events")
                        .header("X-User-Id", "1").header("X-User-Role", "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return JSON.readTree(response).get("id").asLong();
    }

    private void issue(long couponEventId, long userId) throws Exception {
        mockMvc.perform(post("/api/coupons/events/{eventId}/issue", couponEventId)
                        .header("X-User-Id", String.valueOf(userId)))
                .andExpect(status().isAccepted());
    }

    private JsonNode myCoupons(long userId) throws Exception {
        String response = mockMvc.perform(get("/api/coupons/me").header("X-User-Id", String.valueOf(userId)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return JSON.readTree(response);
    }

    private long savedCount(long userId) {
        return jdbc.queryForObject("SELECT count(*) FROM issued_coupons WHERE user_id = ?", Long.class, userId);
    }

    private long insertEventRow(String name, Instant issueStartAt) {
        return jdbc.queryForObject(
                "INSERT INTO coupon_events (name, total_quantity, issue_start_at, created_at)"
                        + " VALUES (?, 10, ?, ?) RETURNING id",
                Long.class, name, Timestamp.from(issueStartAt), Timestamp.from(issueStartAt.minusSeconds(3600)));
    }

    private void insertIssuedRow(long couponEventId, long userId, Instant issuedAt) {
        jdbc.update("INSERT INTO issued_coupons (coupon_event_id, user_id, issued_at) VALUES (?, ?, ?)",
                couponEventId, userId, Timestamp.from(issuedAt));
    }

    @Test
    @DisplayName("[T6-20] 이벤트 2개에서 각각 발급하면 GET /me 는 2건, 이벤트 이름 포함, 최신 발급순. 다른 사용자 쿠폰은 섞이지 않는다")
    void twoEventsLatestFirst() throws Exception {
        // Given: 실제 시각 + 1일에 시작하는 이벤트 2개를 등록(등록 시 Redis 초기화)
        Instant start = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        clock.set(start.minus(1, ChronoUnit.HOURS));
        long spring = registerEvent("봄 쿠폰", start);
        long summer = registerEvent("여름 쿠폰", start);

        // Given: 시작 후 봄 → (1분 뒤) 여름 순으로 발급. 다른 사용자(4)도 봄 쿠폰을 받는다
        clock.set(start.plusSeconds(1));
        issue(spring, 3L);
        issue(spring, 4L);
        clock.set(start.plusSeconds(61));
        issue(summer, 3L);
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200))
                .until(() -> savedCount(3L) == 2 && savedCount(4L) == 1);

        // When
        JsonNode mine = myCoupons(3L);

        // Then
        assertThat(mine.size()).isEqualTo(2);
        assertThat(mine.get(0).get("couponEventId").asLong()).isEqualTo(summer);
        assertThat(mine.get(0).get("eventName").asString()).isEqualTo("여름 쿠폰");
        assertThat(Instant.parse(mine.get(0).get("issuedAt").asString())).isEqualTo(start.plusSeconds(61));
        assertThat(mine.get(1).get("couponEventId").asLong()).isEqualTo(spring);
        assertThat(mine.get(1).get("eventName").asString()).isEqualTo("봄 쿠폰");
        assertThat(Instant.parse(mine.get(1).get("issuedAt").asString())).isEqualTo(start.plusSeconds(1));
        assertThat(myCoupons(4L).size()).isEqualTo(1);
        assertThat(myCoupons(99L).isEmpty()).as("쿠폰 없는 사용자는 빈 배열").isTrue();
    }

    @Test
    @DisplayName("[T6-20] 같은 issuedAt 이면 id 역순(나중 저장 먼저)으로, 반복 조회해도 순서가 같다 (v1.6 보조 정렬)")
    void sameIssuedAtDeterministic() throws Exception {
        // Given: 같은 시각에 발급된 쿠폰 3건을 e1 → e2 → e3 순서로 저장 (id 오름차순 = 저장 순서)
        Instant start = Instant.parse("2026-11-20T09:00:00Z");
        Instant issuedAt = Instant.parse("2026-11-20T10:00:00.123456Z");
        long e1 = insertEventRow("A 쿠폰", start);
        long e2 = insertEventRow("B 쿠폰", start);
        long e3 = insertEventRow("C 쿠폰", start);
        insertIssuedRow(e1, 3L, issuedAt);
        insertIssuedRow(e2, 3L, issuedAt);
        insertIssuedRow(e3, 3L, issuedAt);

        // When: 여러 번 조회
        List<List<Long>> orders = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<Long> order = new ArrayList<>();
            myCoupons(3L).forEach(n -> order.add(n.get("couponEventId").asLong()));
            orders.add(order);
        }

        // Then
        assertThat(orders).allSatisfy(order -> assertThat(order).containsExactly(e3, e2, e1));
    }
}
