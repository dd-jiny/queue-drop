package com.queuedrop.entry.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.ApplicantCounter;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import com.queuedrop.entry.infrastructure.config.RedisScriptConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis 응모자 수 어댑터 통합 테스트 (단계 문서 5단계 3.3 RedisApplicantCounter — 건수 비교의 Redis 쪽 근거).
 * 응모자 Set 은 실제 게이트(Lua)로 만들어 같은 키를 세는지 교차 검증한다.
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisEntryGate.class, RedisApplicantCounter.class, SimpleMeterRegistry.class})
class RedisApplicantCounterTest {

    private static final long DROP_ID = 62L;
    private static final long OTHER_DROP_ID = 63L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-20T11:00:00Z");
    private static final Instant NOW = OPEN_AT.plus(Duration.ofMinutes(30));
    private static final long PAUSE_MS = 1_500L;

    @Autowired
    private ApplicantCounter counter;

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

    /** 단언이 중간에 실패해도 CLIENT PAUSE 가 다음 테스트의 FLUSHALL 을 타임아웃시키지 않게 끝날 때까지 기다린다 */
    @AfterEach
    void waitForPauseEnd() {
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                .until(() -> redis.hasKey("pause-probe") != null);
    }

    private void apply(long dropId, long userId) {
        redis.opsForValue().set("queue:" + dropId + ":token:" + userId, String.valueOf(NOW.toEpochMilli()),
                Duration.ofSeconds(300));
        assertThat(gate.tryApply(dropId, userId, "e-" + userId, NOW).outcome()).isEqualTo(ApplyOutcome.OK);
    }

    @Test
    @DisplayName("[T5-10 보강] (어댑터) 응모 3건 후 count 는 3, 보상 1건 후 2 이고 다른 드로우와 섞이지 않는다")
    void countsApplicantsSet() {
        // Given
        gate.initialize(DROP_ID, 10, OPEN_AT, CLOSE_AT);
        gate.initialize(OTHER_DROP_ID, 10, OPEN_AT, CLOSE_AT);
        apply(DROP_ID, 1L);
        apply(DROP_ID, 2L);
        apply(DROP_ID, 3L);
        apply(OTHER_DROP_ID, 1L);

        // When / Then
        assertThat(counter.count(DROP_ID)).isEqualTo(3L);
        assertThat(counter.count(OTHER_DROP_ID)).isEqualTo(1L);

        // When — 보상으로 응모자 제거
        assertThat(gate.compensate(DROP_ID, 2L, "e-2")).isEqualTo(CompensationResult.COMPENSATED);

        // Then
        assertThat(counter.count(DROP_ID)).isEqualTo(2L);
    }

    @Test
    @DisplayName("[T5-10 보강] (어댑터, 경계) 응모자 키가 없으면(응모 0건·미초기화) 0 이다")
    void zeroWhenKeyMissing() {
        // Given
        gate.initialize(DROP_ID, 10, OPEN_AT, CLOSE_AT);

        // When / Then
        assertThat(counter.count(DROP_ID)).isZero();
        assertThat(counter.count(999L)).isZero();
    }

    @Test
    @DisplayName("[T5-10 보강] (어댑터, 경계) Redis 명령 타임아웃이면 GateUnavailableException(원인 DataAccessException) 으로 감싼다")
    void timeoutBecomesGateUnavailable() {
        // Given
        gate.initialize(DROP_ID, 10, OPEN_AT, CLOSE_AT);
        apply(DROP_ID, 1L);

        RedisClient client = RedisClient.create(
                RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
        Throwable thrown;
        try (StatefulRedisConnection<String, String> pauser = client.connect()) {
            pauser.sync().clientPause(PAUSE_MS);

            // When
            thrown = catchThrowable(() -> counter.count(DROP_ID));
        } finally {
            client.shutdown();
        }

        // Then
        assertThat(thrown).isInstanceOf(GateUnavailableException.class);
        assertThat(thrown.getCause()).isInstanceOf(DataAccessException.class);

        // Then — pause 가 끝나면 다시 정상 조회 (다음 주기 재확인 가능)
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                .until(() -> counter.count(DROP_ID) == 1L);
    }
}
