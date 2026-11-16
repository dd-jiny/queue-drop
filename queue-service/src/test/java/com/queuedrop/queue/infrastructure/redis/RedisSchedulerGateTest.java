package com.queuedrop.queue.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.infrastructure.redis.support.RedisTestSupport;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

@DataRedisTest
@Import(RedisContainerConfig.class)
class RedisSchedulerGateTest {

    private static final String NAME = "admission";
    private static final String GATE_KEY = "queue:admission:gate";
    private static final Duration WINDOW = Duration.ofSeconds(1);

    @Autowired
    private StringRedisTemplate redis;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
    }

    @Test
    @DisplayName("[T3-20] window 1초 — 같은 이름으로 2회 tryPass 하면 true, false")
    void secondPassWithinWindowIsRejected() {
        // Given
        RedisSchedulerGate gate = new RedisSchedulerGate(redis, "instance-A");

        // When
        boolean first = gate.tryPass(NAME, WINDOW);
        boolean second = gate.tryPass(NAME, WINDOW);

        // Then
        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }

    @Test
    @DisplayName("[T3-20] 통과하면 queue:admission:gate 에 인스턴스 ID 가 남고 해제하지 않는다 (만료에 맡김)")
    void gateKeyHoldsInstanceIdUntilExpiry() {
        // Given
        RedisSchedulerGate gate = new RedisSchedulerGate(redis, "instance-A");

        // When
        gate.tryPass(NAME, WINDOW);

        // Then
        assertThat(redis.opsForValue().get(GATE_KEY)).isEqualTo("instance-A");
        assertThat(redis.getExpire(GATE_KEY, java.util.concurrent.TimeUnit.MILLISECONDS))
                .isBetween(1L, WINDOW.toMillis());
    }

    @Test
    @DisplayName("[T3-20] window 가 지나면(약 1.1초 후) 다시 통과한다")
    void passesAgainAfterWindow() {
        // Given
        RedisSchedulerGate gate = new RedisSchedulerGate(redis, "instance-A");
        assertThat(gate.tryPass(NAME, WINDOW)).isTrue();

        // When / Then — 1.1초 이후부터 확인, 최대 3초
        await().pollDelay(Duration.ofMillis(1_100)).atMost(Duration.ofSeconds(3))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> gate.tryPass(NAME, WINDOW));
    }

    @Test
    @DisplayName("[T3-20] 다른 인스턴스도 window 안에서는 같은 이름으로 통과하지 못한다 (전역 게이트)")
    void otherInstanceIsBlocked() {
        // Given
        RedisSchedulerGate gateA = new RedisSchedulerGate(redis, "instance-A");
        RedisSchedulerGate gateB = new RedisSchedulerGate(redis, "instance-B");

        // When
        boolean a = gateA.tryPass(NAME, WINDOW);
        boolean b = gateB.tryPass(NAME, WINDOW);

        // Then
        assertThat(a).isTrue();
        assertThat(b).isFalse();
        assertThat(redis.opsForValue().get(GATE_KEY)).isEqualTo("instance-A");
    }

    @Test
    @DisplayName("[T3-20] 이름이 다르면 게이트는 서로 독립이다")
    void differentNamesAreIndependent() {
        // Given
        RedisSchedulerGate gate = new RedisSchedulerGate(redis, "instance-A");
        gate.tryPass(NAME, WINDOW);

        // When
        boolean other = gate.tryPass("other", WINDOW);

        // Then
        assertThat(other).isTrue();
        assertThat(redis.hasKey("queue:other:gate")).isTrue();
    }

    @Test
    @DisplayName("[T3-20] 기본 생성자는 인스턴스 ID 로 랜덤 UUID 를 쓴다 (인스턴스마다 다름)")
    void defaultConstructorUsesRandomUuid() {
        // Given
        RedisSchedulerGate gate1 = new RedisSchedulerGate(redis);
        RedisSchedulerGate gate2 = new RedisSchedulerGate(redis);

        // When
        gate1.tryPass("one", WINDOW);
        gate2.tryPass("two", WINDOW);
        String id1 = redis.opsForValue().get("queue:one:gate");
        String id2 = redis.opsForValue().get("queue:two:gate");

        // Then
        assertThatCode(() -> UUID.fromString(id1)).doesNotThrowAnyException();
        assertThatCode(() -> UUID.fromString(id2)).doesNotThrowAnyException();
        assertThat(id1).isNotEqualTo(id2);
    }
}
