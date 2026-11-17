package com.queuedrop.coupon.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import com.queuedrop.coupon.infrastructure.config.RedisScriptConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 쿠폰 발급 Lua 게이트 통합 테스트 (단계 문서 4장 T6-08 ~ T6-10, T6-24, T6-28).
 * 키 이름은 phase-6-coupon.md 3.3 / architecture.md 5장 문자열을 테스트가 독립적으로 고정한다 (CouponKeys 와 교차 검증).
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisCouponGate.class})
class RedisCouponGateTest {

    private static final long EVENT_ID = 42L;
    private static final long OTHER_EVENT_ID = 43L;
    private static final long USER_A = 1001L;
    private static final long USER_B = 1002L;

    private static final Instant START_AT = Instant.parse("2026-11-20T10:00:00Z");
    /** 발급 중인 기준 시각 */
    private static final Instant NOW = START_AT.plus(Duration.ofMinutes(5));

    @Autowired
    private RedisCouponGate gate;

    @Autowired
    private StringRedisTemplate redis;

    /** T6-28: CLIENT PAUSE 를 보낼 별도 연결의 접속 정보 */
    @Autowired
    private LettuceConnectionFactory connectionFactory;

    /** T6-28: 잘못된 포트로 만든 Gate 에 같은 스크립트를 넘긴다 */
    @Autowired
    private RedisScript<String> couponIssueScript;

    @Autowired
    private RedisScript<Long> couponCompensateScript;

    @Autowired
    private RedisScript<Long> couponClaimScript;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    // ---- 키 (phase-6-coupon.md 3.3) ----
    private static String metaKey(long id)      { return "coupon:" + id + ":meta"; }
    private static String remainingKey(long id) { return "coupon:" + id + ":remaining"; }
    private static String issuedKey(long id)    { return "coupon:" + id + ":issued"; }
    private static String requestsKey(long id)  { return "coupon:" + id + ":requests"; }
    private static String persistedKey(long id) { return "coupon:" + id + ":persisted"; }

    // ---- 도우미 ----
    private void initialize(int quantity) {
        gate.initialize(EVENT_ID, quantity, START_AT);
    }

    private IssueOutcome issue(long userId, String requestId) {
        return gate.tryIssue(EVENT_ID, userId, requestId, NOW).outcome();
    }

    private static String req(long userId) {
        return "r-" + userId;
    }

    private String remaining() {
        return redis.opsForValue().get(remainingKey(EVENT_ID));
    }

    private boolean isIssued(long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(issuedKey(EVENT_ID), String.valueOf(userId)));
    }

    private Long issuedCount() {
        return redis.opsForSet().size(issuedKey(EVENT_ID));
    }

    private String request(long userId) {
        return (String) redis.opsForHash().get(requestsKey(EVENT_ID), String.valueOf(userId));
    }

    private Map<Object, Object> requests() {
        return redis.opsForHash().entries(requestsKey(EVENT_ID));
    }

    private boolean isPersisted(long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(persistedKey(EVENT_ID), String.valueOf(userId)));
    }

    @Test
    @DisplayName("[T6-08] CouponKeys 는 coupon:{eventId}:meta/remaining/issued/requests/persisted 문자열을 만든다")
    void keysMatchDesign() {
        // Given / When / Then
        assertThat(CouponKeys.meta(EVENT_ID)).isEqualTo(metaKey(EVENT_ID));
        assertThat(CouponKeys.remaining(EVENT_ID)).isEqualTo(remainingKey(EVENT_ID));
        assertThat(CouponKeys.issued(EVENT_ID)).isEqualTo(issuedKey(EVENT_ID));
        assertThat(CouponKeys.requests(EVENT_ID)).isEqualTo(requestsKey(EVENT_ID));
        assertThat(CouponKeys.persisted(EVENT_ID)).isEqualTo(persistedKey(EVENT_ID));
    }

    @Nested
    @DisplayName("초기화")
    class Initialize {

        @Test
        @DisplayName("[T6-10] initialize 는 meta Hash(issueStartAt = epoch ms)와 remaining 정수 문자열을 만든다")
        void createsMetaAndRemaining() {
            // When
            initialize(100);

            // Then
            assertThat(redis.opsForHash().entries(metaKey(EVENT_ID)))
                    .containsExactly(Map.entry("issueStartAt", String.valueOf(START_AT.toEpochMilli())));
            assertThat(remaining()).isEqualTo("100");
            assertThat(redis.hasKey(issuedKey(EVENT_ID))).isFalse();
            assertThat(redis.hasKey(requestsKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-10] 수량 100 초기화 후 30개 발급, initialize 재호출 → 잔여 70 유지 (SET NX)")
        void reinitializeKeepsRemaining() {
            // Given
            initialize(100);
            for (long u = 1; u <= 30; u++) {
                assertThat(issue(u, req(u))).isEqualTo(IssueOutcome.OK);
            }

            // When
            initialize(100);

            // Then
            assertThat(remaining()).isEqualTo("70");
            assertThat(issuedCount()).isEqualTo(30L);
            assertThat(gate.remaining(EVENT_ID)).isEqualTo(70L);
        }

        @Test
        @DisplayName("[T6-10] (경계) 다른 수량·시작 시각으로 재초기화해도 remaining·issueStartAt 은 처음 값 그대로 (HSETNX)")
        void reinitializeWithDifferentValuesKeepsFirst() {
            // Given
            initialize(100);

            // When
            gate.initialize(EVENT_ID, 5, START_AT.plus(Duration.ofHours(1)));

            // Then
            assertThat(remaining()).isEqualTo("100");
            assertThat(redis.opsForHash().get(metaKey(EVENT_ID), "issueStartAt"))
                    .isEqualTo(String.valueOf(START_AT.toEpochMilli()));
        }

        @Test
        @DisplayName("[T6-10] (경계) remaining 은 Redis 잔여를 반환하고, 초기화 전이면 0")
        void remainingReadsRedis() {
            // Given / When / Then — 초기화 전
            assertThat(gate.remaining(EVENT_ID)).isZero();
            assertThat(redis.hasKey(remainingKey(EVENT_ID))).isFalse();

            // Given
            initialize(3);
            issue(USER_A, req(USER_A));

            // When / Then
            assertThat(gate.remaining(EVENT_ID)).isEqualTo(2L);
        }
    }

    @Nested
    @DisplayName("발급 판정")
    class TryIssue {

        @Test
        @DisplayName("[T6-08] 미초기화 이벤트 → NOT_FOUND, 아무 키도 만들지 않는다")
        void notFound() {
            // When
            IssueOutcome outcome = issue(USER_A, req(USER_A));

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.NOT_FOUND);
            assertThat(redis.hasKey(metaKey(EVENT_ID))).isFalse();
            assertThat(redis.hasKey(remainingKey(EVENT_ID))).isFalse();
            assertThat(redis.hasKey(issuedKey(EVENT_ID))).isFalse();
            assertThat(redis.hasKey(requestsKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-08] 시작 전(now = issueStartAt - 1ms) → NOT_STARTED, remaining·issued·requests 변화 없음")
        void notStarted() {
            // Given
            initialize(3);

            // When
            IssueOutcome outcome = gate.tryIssue(EVENT_ID, USER_A, req(USER_A), START_AT.minusMillis(1)).outcome();

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.NOT_STARTED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isIssued(USER_A)).isFalse();
            assertThat(requests()).isEmpty();
        }

        @Test
        @DisplayName("[T6-08] (경계) now = issueStartAt 정각 → OK")
        void okAtStartInstant() {
            // Given
            initialize(3);

            // When
            IssueOutcome outcome = gate.tryIssue(EVENT_ID, USER_A, req(USER_A), START_AT).outcome();

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.OK);
        }

        @Test
        @DisplayName("[T6-08] 첫 발급 → OK, remaining -1(정수 문자열), issued 에 userId, requests[userId] = requestId")
        void firstIssueOk() {
            // Given
            initialize(3);

            // When
            IssueOutcome outcome = issue(USER_A, "r1");

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.OK);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isIssued(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("r1");
            assertThat(isPersisted(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T6-08] 재발급 → DUPLICATE, remaining 변화 없음, 기존 requestId 를 덮어쓰지 않는다")
        void duplicate() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When
            IssueOutcome outcome = issue(USER_A, "r2");

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.DUPLICATE);
            assertThat(remaining()).isEqualTo("2");
            assertThat(issuedCount()).isEqualTo(1L);
            assertThat(request(USER_A)).isEqualTo("r1");
        }

        @Test
        @DisplayName("[T6-08] 수량 소진 → SOLD_OUT, remaining 은 음수가 되지 않고 issued·requests 변화 없음")
        void soldOut() {
            // Given
            initialize(1);
            assertThat(issue(USER_A, req(USER_A))).isEqualTo(IssueOutcome.OK);

            // When
            IssueOutcome outcome = issue(USER_B, req(USER_B));

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.SOLD_OUT);
            assertThat(remaining()).isEqualTo("0");
            assertThat(isIssued(USER_B)).isFalse();
            assertThat(request(USER_B)).isNull();
        }

        @Test
        @DisplayName("[T6-08] (경계) 판정 순서: 중복이 매진보다 먼저 — 마지막 자리를 받은 사용자가 다시 요청하면 DUPLICATE")
        void duplicateBeforeSoldOut() {
            // Given
            initialize(1);
            issue(USER_A, "r1");

            // When / Then
            assertThat(issue(USER_A, "r2")).isEqualTo(IssueOutcome.DUPLICATE);
        }

        @Test
        @DisplayName("[T6-08] (경계) 판정 순서: 시작 전이 중복보다 먼저 — 시작 전이면 발급자여도 NOT_STARTED")
        void notStartedBeforeDuplicate() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When / Then
            assertThat(gate.tryIssue(EVENT_ID, USER_A, "r2", START_AT.minusMillis(1)).outcome())
                    .isEqualTo(IssueOutcome.NOT_STARTED);
        }

        @Test
        @DisplayName("[T6-08] (경계) meta 는 있고 remaining 키만 없음 → SOLD_OUT, remaining 키를 만들지 않는다 (음수 방지)")
        void missingRemainingIsSoldOut() {
            // Given
            initialize(3);
            redis.delete(remainingKey(EVENT_ID));

            // When
            IssueOutcome outcome = issue(USER_A, req(USER_A));

            // Then
            assertThat(outcome).isEqualTo(IssueOutcome.SOLD_OUT);
            assertThat(redis.hasKey(remainingKey(EVENT_ID))).isFalse();
            assertThat(isIssued(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T6-08] (경계) 이벤트별 키가 분리된다 — 다른 이벤트의 발급은 이 이벤트 remaining·issued 에 영향 없음")
        void eventsAreIsolated() {
            // Given
            initialize(2);
            gate.initialize(OTHER_EVENT_ID, 2, START_AT);

            // When
            gate.tryIssue(OTHER_EVENT_ID, USER_A, req(USER_A), NOW);

            // Then
            assertThat(remaining()).isEqualTo("2");
            assertThat(isIssued(USER_A)).isFalse();
            assertThat(issue(USER_A, req(USER_A))).isEqualTo(IssueOutcome.OK);
            assertThat(redis.opsForValue().get(remainingKey(OTHER_EVENT_ID))).isEqualTo("1");
        }
    }

    @Nested
    @DisplayName("보상")
    class Compensate {

        @Test
        @DisplayName("[T6-09] 발급 1건 후 같은 requestId 로 compensate 2번 → COMPENSATED·NOT_OURS, 잔여는 1만 복구, issued·requests 제거")
        void compensateTwice() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When
            CompensationResult first = gate.compensate(EVENT_ID, USER_A, "r1");
            CompensationResult second = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(first).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(second).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isIssued(USER_A)).isFalse();
            assertThat(request(USER_A)).isNull();
        }

        @Test
        @DisplayName("[T6-09] 다른 requestId 로 보상 → NOT_OURS, remaining·issued·requests 변화 없음")
        void otherRequestIdIsNotOurs() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When
            CompensationResult result = gate.compensate(EVENT_ID, USER_A, "r2");

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isIssued(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("r1");
        }

        @Test
        @DisplayName("[T6-09] 판정 없이 compensate → NOT_OURS, 아무 변화 없음 (E-41 판정 미실행 경로)")
        void compensateWithoutIssueIsNotOurs() {
            // Given
            initialize(3);

            // When
            CompensationResult result = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("3");
            assertThat(redis.hasKey(issuedKey(EVENT_ID))).isFalse();
            assertThat(redis.hasKey(requestsKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-09] (경계) 초기화 전 이벤트에 보상 → NOT_OURS, remaining 키를 만들지 않는다")
        void compensateBeforeInitializeCreatesNothing() {
            // When
            CompensationResult result = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(redis.hasKey(remainingKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-09] (경계) 매진 상태에서 보상하면 자리가 하나 생겨 다른 사용자가 OK, 보상된 사용자도 재발급 OK")
        void compensateReopensSeat() {
            // Given
            initialize(1);
            issue(USER_A, "r1");
            assertThat(issue(USER_B, req(USER_B))).isEqualTo(IssueOutcome.SOLD_OUT);

            // When
            gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(issue(USER_B, req(USER_B))).isEqualTo(IssueOutcome.OK);
            assertThat(remaining()).isEqualTo("0");
            gate.compensate(EVENT_ID, USER_B, req(USER_B));
            assertThat(issue(USER_A, "r1-retry")).isEqualTo(IssueOutcome.OK);
            assertThat(request(USER_A)).isEqualTo("r1-retry");
        }

        @Test
        @DisplayName("[T6-09] (경계) 발급(r1) → 보상(r1) → 재발급(r2) 뒤 늦게 온 보상(r1) = NOT_OURS: 재발급 자리(r2)는 그대로")
        void lateCompensateDoesNotUndoReissue() {
            // Given
            initialize(2);
            issue(USER_A, "r1");
            gate.compensate(EVENT_ID, USER_A, "r1");
            issue(USER_A, "r2");

            // When
            CompensationResult late = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(late).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("1");
            assertThat(isIssued(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("r2");
        }
    }

    @Nested
    @DisplayName("저장 확정 claim (E-40)")
    class Claim {

        @Test
        @DisplayName("[T6-24] 발급(r1) 후 claimForSave(r1) → true, persisted 에 userId, 재호출도 true (멱등)")
        void claimSameRequestId() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When
            boolean first = gate.claimForSave(EVENT_ID, USER_A, "r1");
            boolean second = gate.claimForSave(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(first).isTrue();
            assertThat(second).isTrue();
            assertThat(isPersisted(USER_A)).isTrue();
            assertThat(redis.opsForSet().size(persistedKey(EVENT_ID))).isEqualTo(1L);
        }

        @Test
        @DisplayName("[T6-24] 다른 requestId 로 claim → false, persisted 를 만들지 않는다")
        void claimOtherRequestId() {
            // Given
            initialize(3);
            issue(USER_A, "r1");

            // When
            boolean claimed = gate.claimForSave(EVENT_ID, USER_A, "r2");

            // Then
            assertThat(claimed).isFalse();
            assertThat(isPersisted(USER_A)).isFalse();
            assertThat(redis.hasKey(persistedKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-24] 보상 후 claim → false (보상된 발급의 유령 이벤트)")
        void claimAfterCompensate() {
            // Given
            initialize(3);
            issue(USER_A, "r1");
            gate.compensate(EVENT_ID, USER_A, "r1");

            // When
            boolean claimed = gate.claimForSave(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(claimed).isFalse();
            assertThat(isPersisted(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T6-24] (경계) 발급자가 아니면 claim → false")
        void claimWithoutIssue() {
            // Given
            initialize(3);

            // When / Then
            assertThat(gate.claimForSave(EVENT_ID, USER_A, "r1")).isFalse();
            assertThat(redis.hasKey(persistedKey(EVENT_ID))).isFalse();
        }

        @Test
        @DisplayName("[T6-24] (경계) 발급(r1) → 보상(r1) → 재발급(r2) 후 claim(r1) false(유령 이벤트), claim(r2) true")
        void claimGhostAfterReissue() {
            // Given
            initialize(3);
            issue(USER_A, "r1");
            gate.compensate(EVENT_ID, USER_A, "r1");
            issue(USER_A, "r2");

            // When / Then
            assertThat(gate.claimForSave(EVENT_ID, USER_A, "r1")).isFalse();
            assertThat(isPersisted(USER_A)).isFalse();
            assertThat(gate.claimForSave(EVENT_ID, USER_A, "r2")).isTrue();
            assertThat(isPersisted(USER_A)).isTrue();
        }

        @Test
        @DisplayName("[T6-24] claim 후 보상 → ALREADY_PERSISTED, 수량·issued·requests·persisted 유지")
        void compensateAfterClaimIsAlreadyPersisted() {
            // Given
            initialize(3);
            issue(USER_A, "r1");
            gate.claimForSave(EVENT_ID, USER_A, "r1");

            // When
            CompensationResult result = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.ALREADY_PERSISTED);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isIssued(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("r1");
            assertThat(isPersisted(USER_A)).isTrue();
        }

        @Test
        @DisplayName("[T6-24] (경계) ALREADY_PERSISTED 는 requestId 대조보다 먼저 — 확정 후 다른 requestId 로 보상해도 ALREADY_PERSISTED")
        void alreadyPersistedBeforeRequestIdCheck() {
            // Given
            initialize(3);
            issue(USER_A, "r1");
            gate.claimForSave(EVENT_ID, USER_A, "r1");

            // When / Then
            assertThat(gate.compensate(EVENT_ID, USER_A, "r2")).isEqualTo(CompensationResult.ALREADY_PERSISTED);
            assertThat(remaining()).isEqualTo("2");
        }

        @Test
        @DisplayName("[T6-24] (경계) claim 은 사용자별 — B 의 requestId 로 A 를 claim 하면 false, A 확정 후에도 B 보상은 COMPENSATED")
        void claimIsPerUser() {
            // Given
            initialize(3);
            issue(USER_A, req(USER_A));
            issue(USER_B, req(USER_B));

            // When / Then
            assertThat(gate.claimForSave(EVENT_ID, USER_A, req(USER_B))).isFalse();
            assertThat(gate.claimForSave(EVENT_ID, USER_A, req(USER_A))).isTrue();
            assertThat(gate.compensate(EVENT_ID, USER_B, req(USER_B))).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isPersisted(USER_B)).isFalse();
        }
    }

    @Nested
    @DisplayName("Redis 장애 (E-41)")
    class RedisFailure {

        /** 명령 타임아웃(spring.data.redis.timeout=1s)보다 길게 */
        private static final long PAUSE_MS = 1_500L;

        /** 단언이 중간에 실패해도 pause 가 다음 테스트의 FLUSHALL 을 타임아웃시키지 않게 끝날 때까지 기다린다 (쓰기 명령으로 확인) */
        @AfterEach
        void waitForPauseEnd() {
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> {
                        redis.opsForValue().set("pause-probe", "1");
                        return true;
                    });
        }

        /** 별도 연결로 CLIENT PAUSE {ms} WRITE 를 보낸 뒤 action 을 실행한다 (단계 문서 T6-28) */
        private void withWritePause(Runnable action) {
            RedisClient client = RedisClient.create(
                    RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
            try (StatefulRedisConnection<String, String> pauser = client.connect()) {
                pauser.sync().dispatch(CommandType.CLIENT, new StatusOutput<>(StringCodec.UTF8),
                        new CommandArgs<>(StringCodec.UTF8).add("PAUSE").add(PAUSE_MS).add("WRITE"));
                action.run();
            } finally {
                client.shutdown();
            }
        }

        private static void assertGateUnavailable(Throwable thrown) {
            assertThat(thrown).isInstanceOf(GateUnavailableException.class);
            assertThat(thrown.getCause()).isInstanceOf(DataAccessException.class);
        }

        @Test
        @DisplayName("[T6-28] CLIENT PAUSE WRITE 로 발급 Lua 가 타임아웃되면 GateUnavailableException(원인 DataAccessException), pause 후 Lua 는 실행돼 있고 같은 requestId 보상은 COMPENSATED")
        void issueTimeoutBecomesGateUnavailable() {
            // Given — 발급 가능 상태, 스크립트 캐시 적재
            // (캐시가 없으면 pause 중 EVALSHA 가 타임아웃되고 EVAL 재시도가 없어 Lua 가 실행되지 않는다)
            initialize(3);
            assertThat(gate.tryIssue(OTHER_EVENT_ID, USER_B, "r-warmup", NOW).outcome())
                    .isEqualTo(IssueOutcome.NOT_FOUND);

            // When
            Throwable[] thrown = new Throwable[1];
            withWritePause(() -> thrown[0] = catchThrowable(() -> gate.tryIssue(EVENT_ID, USER_A, "r1", NOW)));

            // Then — 포트 예외로 감싸진다
            assertGateUnavailable(thrown[0]);

            // Then — pause 가 끝나면 늦게 도착한 발급 Lua 는 실행돼 있다
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> isIssued(USER_A));
            assertThat(request(USER_A)).isEqualTo("r1");
            assertThat(remaining()).isEqualTo("2");

            // When — 서비스가 같은 requestId 로 보상
            CompensationResult result = gate.compensate(EVENT_ID, USER_A, "r1");

            // Then — 원상복구
            assertThat(result).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isIssued(USER_A)).isFalse();
            assertThat(request(USER_A)).isNull();
        }

        @Test
        @DisplayName("[T6-28] (경계) 저장 확정 claim·보상도 Redis 타임아웃이면 GateUnavailableException(원인 DataAccessException)")
        void claimAndCompensateTimeoutBecomeGateUnavailable() {
            // Given — 발급(r1) 완료, claim·compensate 스크립트 캐시 적재
            initialize(3);
            issue(USER_A, "r1");
            assertThat(gate.claimForSave(EVENT_ID, USER_B, "r-warmup")).isFalse();
            assertThat(gate.compensate(EVENT_ID, USER_B, "r-warmup")).isEqualTo(CompensationResult.NOT_OURS);

            // When / Then — claim 타임아웃
            Throwable[] claimThrown = new Throwable[1];
            withWritePause(() -> claimThrown[0] = catchThrowable(() -> gate.claimForSave(EVENT_ID, USER_A, "r1")));
            assertGateUnavailable(claimThrown[0]);
            waitForPauseEnd();

            // When / Then — 보상 타임아웃
            Throwable[] compensateThrown = new Throwable[1];
            withWritePause(() -> compensateThrown[0] = catchThrowable(() -> gate.compensate(EVENT_ID, USER_A, "r-other")));
            assertGateUnavailable(compensateThrown[0]);
        }

        @Test
        @DisplayName("[T6-28] 잘못된 포트(연결 불가)면 tryIssue·compensate·claimForSave·initialize·remaining 모두 GateUnavailableException(원인 DataAccessException)")
        void unreachableRedisBecomesGateUnavailable() throws IOException {
            // Given — 아무도 듣지 않는 포트로 연결하는 Gate
            int closedPort;
            try (ServerSocket socket = new ServerSocket(0)) {
                closedPort = socket.getLocalPort();
            }
            LettuceConnectionFactory badFactory = new LettuceConnectionFactory(
                    new RedisStandaloneConfiguration(connectionFactory.getHostName(), closedPort),
                    LettuceClientConfiguration.builder().commandTimeout(Duration.ofSeconds(1)).build());
            badFactory.afterPropertiesSet();
            badFactory.start();
            try {
                RedisCouponGate badGate = new RedisCouponGate(new StringRedisTemplate(badFactory),
                        couponIssueScript, couponCompensateScript, couponClaimScript);

                Map<String, Consumer<RedisCouponGate>> calls = Map.of(
                        "tryIssue", g -> g.tryIssue(EVENT_ID, USER_A, "r1", NOW),
                        "compensate", g -> g.compensate(EVENT_ID, USER_A, "r1"),
                        "claimForSave", g -> g.claimForSave(EVENT_ID, USER_A, "r1"),
                        "initialize", g -> g.initialize(EVENT_ID, 3, START_AT),
                        "remaining", g -> g.remaining(EVENT_ID));

                // When / Then
                calls.forEach((name, call) -> {
                    Throwable thrown = catchThrowable(() -> call.accept(badGate));
                    assertThat(thrown).as(name).isInstanceOf(GateUnavailableException.class);
                    assertThat(thrown.getCause()).as(name).isInstanceOf(DataAccessException.class);
                });
            } finally {
                badFactory.destroy();
            }
        }
    }
}
