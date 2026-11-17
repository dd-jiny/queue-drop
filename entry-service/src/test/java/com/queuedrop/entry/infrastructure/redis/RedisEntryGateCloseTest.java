package com.queuedrop.entry.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import com.queuedrop.entry.infrastructure.config.RedisScriptConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 마감 잠금·추첨 후 키 만료 통합 테스트 (단계 문서 5단계 4장 T5-19, T5-31).
 * 키 이름은 architecture.md 5장 문자열을 테스트가 독립적으로 고정한다 (EntryRedisKeys 와 교차 검증).
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisEntryGate.class, SimpleMeterRegistry.class})
class RedisEntryGateCloseTest {

    private static final long DROP_ID = 52L;
    private static final long OTHER_DROP_ID = 53L;
    private static final long USER_A = 2001L;
    private static final long USER_B = 2002L;
    private static final Duration ONE_DAY = Duration.ofDays(1);

    private static final Instant OPEN_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-20T11:00:00Z");
    /** 오픈 중(closeAt 전)인 기준 시각 — close 효과를 시각 검사와 분리해 본다 */
    private static final Instant NOW = OPEN_AT.plus(Duration.ofMinutes(30));

    @Autowired
    private RedisEntryGate gate;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private LettuceConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    // ---- 키 (architecture.md 5장) ----
    private static List<String> entryKeys(long dropId) {
        String p = "entry:" + dropId + ":";
        return List.of(p + "meta", p + "remaining", p + "applicants", p + "seq", p + "requests", p + "persisted");
    }

    private static String metaKey(long dropId) {
        return "entry:" + dropId + ":meta";
    }

    private static String tokenKey(long dropId, long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }

    // ---- 도우미 ----
    private void initialize(long dropId) {
        gate.initialize(dropId, 3, OPEN_AT, CLOSE_AT);
    }

    private void issueToken(long dropId, long userId) {
        redis.opsForValue().set(tokenKey(dropId, userId), String.valueOf(NOW.toEpochMilli()),
                Duration.ofSeconds(300));
    }

    /** 초 단위 TTL. -1 = 만료 없음, -2 = 키 없음 */
    private long ttl(String key) {
        Long ttl = redis.getExpire(key);
        return ttl == null ? -2 : ttl;
    }

    @Nested
    @DisplayName("마감 잠금 close")
    class Close {

        @Test
        @DisplayName("[T5-19] OPEN 드로우에서 토큰을 가진 사용자도 close 후 tryApply 는 CLOSED 이고 정원·응모자는 그대로다")
        void tryApplyAfterCloseIsClosed() {
            // Given — closeAt 전, 토큰 보유 (close 가 없으면 OK 가 나는 상태)
            initialize(DROP_ID);
            issueToken(DROP_ID, USER_A);

            // When
            gate.close(DROP_ID);
            ApplyOutcome outcome = gate.tryApply(DROP_ID, USER_A, "e-a", NOW).outcome();

            // Then
            assertThat(outcome).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(redis.opsForHash().get(metaKey(DROP_ID), "state")).isEqualTo("CLOSED");
            assertThat(redis.opsForValue().get("entry:" + DROP_ID + ":remaining")).isEqualTo("3");
            assertThat(redis.hasKey("entry:" + DROP_ID + ":applicants")).isFalse();
        }

        @Test
        @DisplayName("[T5-19] (경계) close 전 응모는 OK, close 후 다른 사용자는 CLOSED — 이미 응모한 사람은 응모자로 남는다")
        void closeKeepsExistingApplicants() {
            // Given
            initialize(DROP_ID);
            issueToken(DROP_ID, USER_A);
            issueToken(DROP_ID, USER_B);
            assertThat(gate.tryApply(DROP_ID, USER_A, "e-a", NOW).outcome()).isEqualTo(ApplyOutcome.OK);

            // When
            gate.close(DROP_ID);

            // Then
            assertThat(gate.tryApply(DROP_ID, USER_B, "e-b", NOW).outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(redis.opsForSet().isMember("entry:" + DROP_ID + ":applicants", String.valueOf(USER_A))).isTrue();
            assertThat(redis.opsForValue().get("entry:" + DROP_ID + ":remaining")).isEqualTo("2");
        }

        @Test
        @DisplayName("[T5-19] (경계) close 는 meta 의 state 만 바꾸고 openAt·closeAt·capacity 필드는 유지한다. 두 번 호출해도 안전하다")
        void closeOnlyChangesStateAndIsIdempotent() {
            // Given
            initialize(DROP_ID);
            var before = redis.opsForHash().entries(metaKey(DROP_ID));

            // When
            gate.close(DROP_ID);
            gate.close(DROP_ID);

            // Then
            var after = redis.opsForHash().entries(metaKey(DROP_ID));
            assertThat(after.keySet()).isEqualTo(before.keySet());
            before.forEach((field, value) -> {
                if (!"state".equals(field)) {
                    assertThat(after.get(field)).as("meta.%s", field).isEqualTo(value);
                }
            });
            assertThat(after.get("state")).isEqualTo("CLOSED");
        }

        @Test
        @DisplayName("[T5-19] (경계) 한 드로우의 close 는 다른 드로우의 응모에 영향을 주지 않는다")
        void closeIsPerDrop() {
            // Given
            initialize(DROP_ID);
            initialize(OTHER_DROP_ID);
            issueToken(OTHER_DROP_ID, USER_A);

            // When
            gate.close(DROP_ID);

            // Then
            assertThat(gate.tryApply(OTHER_DROP_ID, USER_A, "e-a", NOW).outcome()).isEqualTo(ApplyOutcome.OK);
        }
    }

    @Nested
    @DisplayName("추첨 후 키 만료 expireAfterDraw (E-42)")
    class ExpireAfterDraw {

        @Test
        @DisplayName("[T5-31] 응모 1건(6개 키 모두 존재) 드로우에 expireAfterDraw(1d) 후 6개 키 TTL 이 0 초과 86400 이하다")
        void expiresAllSixKeys() {
            // Given — initialize(meta, remaining) + 응모(applicants, seq, requests) + 저장 확정(persisted)
            initialize(DROP_ID);
            issueToken(DROP_ID, USER_A);
            assertThat(gate.tryApply(DROP_ID, USER_A, "e-a", NOW).outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(gate.claimForSave(DROP_ID, USER_A, "e-a")).isTrue();
            for (String key : entryKeys(DROP_ID)) {
                assertThat(ttl(key)).as("준비: %s 는 만료 없이 존재", key).isEqualTo(-1L);
            }

            // When
            gate.expireAfterDraw(DROP_ID, ONE_DAY);

            // Then
            for (String key : entryKeys(DROP_ID)) {
                assertThat(ttl(key)).as("TTL of %s", key).isGreaterThan(0L).isLessThanOrEqualTo(86_400L);
            }
        }

        @Test
        @DisplayName("[T5-31] 응모 0건 드로우(meta·remaining 만 존재)도 예외 없이 있는 키에만 TTL 이 걸리고 없는 키는 만들지 않는다")
        void noApplicantsDoesNotFail() {
            // Given
            initialize(DROP_ID);

            // When / Then
            assertThatCode(() -> gate.expireAfterDraw(DROP_ID, ONE_DAY)).doesNotThrowAnyException();
            assertThat(ttl(metaKey(DROP_ID))).isGreaterThan(0L).isLessThanOrEqualTo(86_400L);
            assertThat(ttl("entry:" + DROP_ID + ":remaining")).isGreaterThan(0L).isLessThanOrEqualTo(86_400L);
            for (String missing : List.of("applicants", "seq", "requests", "persisted")) {
                assertThat(redis.hasKey("entry:" + DROP_ID + ":" + missing)).as(missing).isFalse();
            }
        }

        @Test
        @DisplayName("[T5-31] (경계) 키가 하나도 없는 드로우에도 예외 없이 끝난다")
        void noKeysDoesNotFail() {
            // When / Then
            assertThatCode(() -> gate.expireAfterDraw(DROP_ID, ONE_DAY)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("[T5-31] (경계) 만료는 TTL 인자를 따르고(1시간이면 3600 이하), 키 값은 바꾸지 않는다")
        void ttlFollowsArgumentAndKeepsValues() {
            // Given
            initialize(DROP_ID);
            issueToken(DROP_ID, USER_A);
            gate.tryApply(DROP_ID, USER_A, "e-a", NOW);

            // When
            gate.expireAfterDraw(DROP_ID, Duration.ofHours(1));

            // Then
            assertThat(ttl(metaKey(DROP_ID))).isGreaterThan(0L).isLessThanOrEqualTo(3_600L);
            assertThat(redis.opsForValue().get("entry:" + DROP_ID + ":remaining")).isEqualTo("2");
            assertThat(redis.opsForValue().get("entry:" + DROP_ID + ":seq")).isEqualTo("1");
            assertThat(redis.opsForHash().get("entry:" + DROP_ID + ":requests", String.valueOf(USER_A))).isEqualTo("e-a");
        }

        @Test
        @DisplayName("[T5-31] (경계) queue 소유 토큰 키와 다른 드로우의 키는 건드리지 않는다")
        void doesNotTouchQueueTokenOrOtherDrop() {
            // Given
            initialize(DROP_ID);
            initialize(OTHER_DROP_ID);
            issueToken(DROP_ID, USER_A);
            gate.tryApply(DROP_ID, USER_A, "e-a", NOW);

            // When
            gate.expireAfterDraw(DROP_ID, ONE_DAY);

            // Then — 토큰 TTL 은 발급 시 300초 그대로, 다른 드로우는 만료 없음
            assertThat(ttl(tokenKey(DROP_ID, USER_A))).isGreaterThan(0L).isLessThanOrEqualTo(300L);
            for (String key : List.of(metaKey(OTHER_DROP_ID), "entry:" + OTHER_DROP_ID + ":remaining")) {
                assertThat(ttl(key)).as(key).isEqualTo(-1L);
            }
        }
    }

    @Nested
    @DisplayName("Redis 명령 타임아웃 (E-41, E-42)")
    class CommandTimeout {

        /** 명령 타임아웃(spring.data.redis.timeout=1s)보다 길게 */
        private static final long PAUSE_MS = 1_500L;

        @AfterEach
        void waitForPauseEnd() {
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> redis.hasKey("pause-probe") != null);
        }

        private Throwable underPause(Runnable call) {
            RedisClient client = RedisClient.create(
                    RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
            try (StatefulRedisConnection<String, String> pauser = client.connect()) {
                pauser.sync().clientPause(PAUSE_MS);
                return catchThrowable(call::run);
            } finally {
                client.shutdown();
            }
        }

        @Test
        @DisplayName("[T5-31] (경계) expireAfterDraw 가 Redis 타임아웃이면 GateUnavailableException(원인 DataAccessException) 으로 감싼다")
        void expireTimeoutBecomesGateUnavailable() {
            // Given
            initialize(DROP_ID);

            // When
            Throwable thrown = underPause(() -> gate.expireAfterDraw(DROP_ID, ONE_DAY));

            // Then
            assertThat(thrown).isInstanceOf(GateUnavailableException.class);
            assertThat(thrown.getCause()).isInstanceOf(DataAccessException.class);
        }

        @Test
        @DisplayName("[T5-19] (경계) close 가 Redis 타임아웃이면 GateUnavailableException(원인 DataAccessException) 으로 감싼다")
        void closeTimeoutBecomesGateUnavailable() {
            // Given
            initialize(DROP_ID);

            // When
            Throwable thrown = underPause(() -> gate.close(DROP_ID));

            // Then
            assertThat(thrown).isInstanceOf(GateUnavailableException.class);
            assertThat(thrown.getCause()).isInstanceOf(DataAccessException.class);
        }
    }
}
