package com.queuedrop.entry.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.EntryGate;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/**
 * drop.created 수신 시나리오 (단계 문서 4장 T4-32, T4-33).
 * EntryCreatedListenerTest 와 같은 @Import 조합이라 컨텍스트를 공유한다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        ListenerTestSupport.TopicConfig.class})
class DropCreatedRedeliveryTest {

    private static final Duration WAIT = Duration.ofSeconds(15);
    private static final Instant OPEN_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-20T12:00:00Z");
    /** 게이트 판정 시각 (응모 기간 안) */
    private static final Instant NOW = Instant.parse("2026-11-20T10:30:00Z");

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired StringRedisTemplate redis;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaContainer kafka;
    @Autowired EntryGate entryGate;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE entries, drop_snapshots");
    }

    private void publish(DropCreated event) throws Exception {
        kafkaTemplate.send(Topics.DROP_CREATED, String.valueOf(event.dropId()), event).get(5, TimeUnit.SECONDS);
    }

    private DropCreated event(long dropId, int capacity, int winners) {
        return new DropCreated(dropId, 3L, "한정판 스니커즈", capacity, winners, OPEN_AT, CLOSE_AT);
    }

    private boolean metaExists(long dropId) {
        return Boolean.TRUE.equals(redis.hasKey("entry:" + dropId + ":meta"));
    }

    private long snapshotRows(long dropId) {
        return jdbc.queryForObject("SELECT count(*) FROM drop_snapshots WHERE drop_id = ?", Long.class, dropId);
    }

    /** drop.created 처리 완료 = DB 스냅샷 + Redis meta 둘 다 생김 (DB 저장 → Redis 초기화 순서) */
    private void awaitRegistered(long dropId) {
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> snapshotRows(dropId) == 1 && metaExists(dropId));
    }

    @Test
    @DisplayName("[T4-32] drop.created 처리 후 30명이 응모한 상태에서 같은 drop.created 를 다시 받아도 remaining=70, drop_snapshots 1행이다")
    void redeliveryDoesNotResetRemaining() throws Exception {
        // Given — 정원 100 드로우 등록 후 30명 응모 (입장 토큰을 심고 게이트로 판정)
        long dropId = 301L;
        DropCreated message = event(dropId, 100, 10);
        publish(message);
        awaitRegistered(dropId);
        for (long userId = 1; userId <= 30; userId++) {
            redis.opsForValue().set("queue:" + dropId + ":token:" + userId, "1", Duration.ofMinutes(5));
            assertThat(entryGate.tryApply(dropId, userId, "e-" + userId, NOW).isOk()).isTrue();
        }
        assertThat(redis.opsForValue().get("entry:" + dropId + ":remaining")).isEqualTo("70");

        // When — 같은 메시지 재수신, 이어서 확인용 메시지 (파티션 1개라 순서대로 처리)
        publish(message);
        publish(event(302L, 100, 10));
        awaitRegistered(302L);

        // Then
        assertThat(redis.opsForValue().get("entry:" + dropId + ":remaining")).isEqualTo("70");
        assertThat(redis.opsForSet().size("entry:" + dropId + ":applicants")).isEqualTo(30);
        assertThat(snapshotRows(dropId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT entry_capacity FROM drop_snapshots WHERE drop_id = ?",
                Integer.class, dropId)).isEqualTo(100);
    }

    @Test
    @DisplayName("[T4-32] (v1.3) 정원이 다른 drop.created 를 다시 받아도 Redis remaining·meta 는 처음(DB) 값 그대로다")
    void redeliveryWithDifferentCapacityKeepsRedis() throws Exception {
        // Given — 정원 100 으로 등록 후 30명 응모
        long dropId = 311L;
        publish(event(dropId, 100, 10));
        awaitRegistered(dropId);
        for (long userId = 1; userId <= 30; userId++) {
            redis.opsForValue().set("queue:" + dropId + ":token:" + userId, "1", Duration.ofMinutes(5));
            assertThat(entryGate.tryApply(dropId, userId, "e-" + userId, NOW).isOk()).isTrue();
        }

        // When — 같은 dropId, 정원 50 인 메시지 재수신 → 확인용 메시지
        publish(event(dropId, 50, 10));
        publish(event(312L, 100, 10));
        awaitRegistered(312L);

        // Then — Redis·DB 모두 처음 값(정원 100) 기준
        assertThat(redis.opsForValue().get("entry:" + dropId + ":remaining")).isEqualTo("70");
        assertThat(redis.<String, String>opsForHash().get("entry:" + dropId + ":meta", "capacity")).isEqualTo("100");
        assertThat(jdbc.queryForObject("SELECT entry_capacity FROM drop_snapshots WHERE drop_id = ?",
                Integer.class, dropId)).isEqualTo(100);
    }

    @Test
    @DisplayName("[T4-32] (v1.3) DB 저장 후 Redis 초기화가 빠진 상태에서 정원이 다른 drop.created 가 오면 Redis 는 DB 값(정원 100)으로 초기화된다")
    void redeliveryAfterRedisInitMissInitializesFromDb() throws Exception {
        // Given — DB 에만 정원 100 스냅샷이 있고 Redis 는 비어 있다 (저장 후 Redis 초기화 실패 상황)
        long dropId = 321L;
        jdbc.update("INSERT INTO drop_snapshots (drop_id, entry_capacity, winner_count, open_at, close_at, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                dropId, 100, 10, java.sql.Timestamp.from(OPEN_AT), java.sql.Timestamp.from(CLOSE_AT), "OPEN");
        assertThat(metaExists(dropId)).isFalse();

        // When — 같은 dropId, 정원 50 인 메시지 수신
        publish(event(dropId, 50, 10));

        // Then — Redis 는 메시지(50)가 아니라 DB(100) 값으로 초기화된다
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100)).until(() -> metaExists(dropId));
        assertThat(redis.opsForValue().get("entry:" + dropId + ":remaining")).isEqualTo("100");
        assertThat(redis.<String, String>opsForHash().get("entry:" + dropId + ":meta", "capacity")).isEqualTo("100");
        assertThat(snapshotRows(dropId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT entry_capacity FROM drop_snapshots WHERE drop_id = ?",
                Integer.class, dropId)).isEqualTo(100);
    }

    @Test
    @DisplayName("[T4-33] capacity < winner 인 drop.created 는 재시도·DLT 없이 스킵되고, 뒤이은 정상 drop.created 는 처리된다")
    void invalidDropCreatedIsSkippedWithoutDlt() throws Exception {
        // Given
        long invalidDrop = 401L;
        long validDrop = 402L;
        String invalidKey = String.valueOf(invalidDrop);

        try (var dlt = new ListenerTestSupport.DltReader(kafka.getBootstrapServers(), Topics.DROP_CREATED_DLT)) {
            // When — 잘못된 메시지(정원 5 < 당첨 10) 다음에 정상 메시지
            publish(event(invalidDrop, 5, 10));
            publish(event(validDrop, 100, 10));

            // Then — 정상 건 처리됨 (파티션 1개라 잘못된 건의 처리도 이미 끝남)
            awaitRegistered(validDrop);
            assertThat(redis.opsForValue().get("entry:" + validDrop + ":remaining")).isEqualTo("100");

            // Then — 잘못된 건은 DB·Redis 어디에도 없다
            assertThat(snapshotRows(invalidDrop)).isZero();
            assertThat(metaExists(invalidDrop)).isFalse();
            assertThat(redis.hasKey("entry:" + invalidDrop + ":remaining")).isFalse();

            // Then — DLT 로 가지 않았다 (잠시 지켜봐도 0건)
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                    .until(() -> dlt.recordsWithKey(invalidKey).isEmpty());
        }
    }
}
