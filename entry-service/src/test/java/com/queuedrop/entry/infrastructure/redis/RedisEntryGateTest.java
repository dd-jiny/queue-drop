package com.queuedrop.entry.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import com.queuedrop.entry.infrastructure.config.RedisScriptConfig;
import java.time.Duration;
import java.time.Instant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 응모 Lua 게이트 통합 테스트 (단계 문서 4장 T4-11~18, v1.5 T4-36~39).
 * 키 이름은 architecture.md 5장 문자열을 테스트가 독립적으로 고정한다 (EntryRedisKeys 와 교차 검증).
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisEntryGate.class})
class RedisEntryGateTest {

    private static final long DROP_ID = 42L;
    private static final long USER_A = 1001L;
    private static final long USER_B = 1002L;

    @Autowired
    private RedisEntryGate gate;

    @Autowired
    private StringRedisTemplate redis;

    /** T4-39: CLIENT PAUSE 를 보낼 별도 연결의 접속 정보 */
    @Autowired
    private LettuceConnectionFactory connectionFactory;

    /** T4-18 의 "반환 0" 은 포트가 void 라 스크립트를 직접 실행해 확인한다 */
    @Autowired
    private RedisScript<Long> entryInitializeScript;

    private static final Instant OPEN_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-20T11:00:00Z");
    /** 오픈 중인 기준 시각 */
    private static final Instant NOW = OPEN_AT.plus(Duration.ofMinutes(30));

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    // ---- 키 (architecture.md 5장) ----
    private static String metaKey(long dropId)       { return "entry:" + dropId + ":meta"; }
    private static String remainingKey(long dropId)  { return "entry:" + dropId + ":remaining"; }
    private static String applicantsKey(long dropId) { return "entry:" + dropId + ":applicants"; }
    private static String seqKey(long dropId)        { return "entry:" + dropId + ":seq"; }
    private static String requestsKey(long dropId)   { return "entry:" + dropId + ":requests"; }
    private static String persistedKey(long dropId)  { return "entry:" + dropId + ":persisted"; }
    private static String tokenKey(long dropId, long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }

    // ---- 도우미 ----
    private void initialize(int capacity) {
        gate.initialize(DROP_ID, capacity, OPEN_AT, CLOSE_AT);
    }

    /** queue admit.lua 와 같은 형태: SET ... EX 300, 값 = 발급 시각 ms */
    private void issueToken(long userId) {
        redis.opsForValue().set(tokenKey(DROP_ID, userId), String.valueOf(NOW.toEpochMilli()),
                Duration.ofSeconds(300));
    }

    private String remaining() {
        return redis.opsForValue().get(remainingKey(DROP_ID));
    }

    private String seq() {
        return redis.opsForValue().get(seqKey(DROP_ID));
    }

    private boolean isMember(long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(applicantsKey(DROP_ID), String.valueOf(userId)));
    }

    /** 사용자별 기본 요청 ID. 재응모처럼 다른 요청을 표현할 때는 문자열을 직접 넘긴다 */
    private static String eventId(long userId) {
        return "e-" + userId;
    }

    private String request(long userId) {
        return (String) redis.opsForHash().get(requestsKey(DROP_ID), String.valueOf(userId));
    }

    private Map<Object, Object> requests() {
        return redis.opsForHash().entries(requestsKey(DROP_ID));
    }

    private boolean isPersisted(long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(persistedKey(DROP_ID), String.valueOf(userId)));
    }

    private GateResult applyWithToken(long userId, String eventId) {
        issueToken(userId);
        return gate.tryApply(DROP_ID, userId, eventId, NOW);
    }

    private GateResult applyWithToken(long userId) {
        issueToken(userId);
        return gate.tryApply(DROP_ID, userId, eventId(userId), NOW);
    }

    @Nested
    @DisplayName("초기화")
    class Initialize {

        @Test
        @DisplayName("[T4-11] initialize 는 meta Hash(state=OPEN, capacity, openAt·closeAt ms)와 remaining 정수 문자열을 만들고 seq 는 만들지 않는다")
        void initializeWritesMetaAndRemaining() {
            // When
            initialize(3);

            // Then
            assertThat(redis.<String, String>opsForHash().entries(metaKey(DROP_ID)))
                    .containsEntry("state", "OPEN")
                    .containsEntry("capacity", "3")
                    .containsEntry("openAt", String.valueOf(OPEN_AT.toEpochMilli()))
                    .containsEntry("closeAt", String.valueOf(CLOSE_AT.toEpochMilli()));
            assertThat(remaining()).isEqualTo("3");
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
            // 문서에 TTL 이 없다: 응모 키는 만료되지 않는다 (만료되면 마감 전 CLOSED/SOLD_OUT 오판)
            assertThat(redis.getExpire(metaKey(DROP_ID))).isEqualTo(-1L);
            assertThat(redis.getExpire(remainingKey(DROP_ID))).isEqualTo(-1L);
        }

        @Test
        @DisplayName("[T4-18] 정원 100 초기화 후 응모 30건, 같은 정원으로 재초기화해도 remaining=70 유지되고 스크립트는 0 을 반환한다")
        void reinitializeDoesNotOverwrite() {
            // Given
            initialize(100);
            LongStream.rangeClosed(1, 30).forEach(userId ->
                    assertThat(applyWithToken(userId).outcome()).isEqualTo(ApplyOutcome.OK));
            assertThat(remaining()).isEqualTo("70");

            // When
            gate.initialize(DROP_ID, 100, OPEN_AT, CLOSE_AT);
            Long direct = redis.execute(entryInitializeScript,
                    List.of(metaKey(DROP_ID), remainingKey(DROP_ID)),
                    "100", String.valueOf(OPEN_AT.toEpochMilli()), String.valueOf(CLOSE_AT.toEpochMilli()));

            // Then
            assertThat(direct).isZero();
            assertThat(remaining()).isEqualTo("70");                    // 10진 정수 문자열 (queue 계약)
            assertThat(Long.parseLong(remaining())).isEqualTo(70L);
            assertThat(redis.opsForSet().size(applicantsKey(DROP_ID))).isEqualTo(30L);
            assertThat(seq()).isEqualTo("30");
        }

        @Test
        @DisplayName("[T4-18] 재초기화가 다른 값(정원·시각)이어도 meta·remaining 은 처음 값 그대로다")
        void reinitializeWithDifferentValuesKeepsFirst() {
            // Given
            initialize(100);

            // When
            gate.initialize(DROP_ID, 5, OPEN_AT.plusSeconds(1), CLOSE_AT.plusSeconds(1));

            // Then
            assertThat(remaining()).isEqualTo("100");
            assertThat(redis.<String, String>opsForHash().entries(metaKey(DROP_ID)))
                    .containsEntry("capacity", "100")
                    .containsEntry("openAt", String.valueOf(OPEN_AT.toEpochMilli()))
                    .containsEntry("closeAt", String.valueOf(CLOSE_AT.toEpochMilli()));
        }

        @Test
        @DisplayName("[T4-18] 처음 초기화 시 스크립트는 1 을 반환한다")
        void firstInitializeReturnsOne() {
            // When
            Long direct = redis.execute(entryInitializeScript,
                    List.of(metaKey(DROP_ID), remainingKey(DROP_ID)),
                    "10", String.valueOf(OPEN_AT.toEpochMilli()), String.valueOf(CLOSE_AT.toEpochMilli()));

            // Then
            assertThat(direct).isEqualTo(1L);
            assertThat(remaining()).isEqualTo("10");
        }
    }

    @Nested
    @DisplayName("응모 판정")
    class TryApply {

        @Test
        @DisplayName("[T4-11] 정원 3, 토큰 보유 → OK(seq=1), remaining=\"2\"(정수 문자열), applicants 에 userId")
        void okFirstApplicant() {
            // Given
            initialize(3);
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(result.entrySeq()).isEqualTo(1);
            assertThat(result.isOk()).isTrue();
            assertThat(remaining()).isEqualTo("2");
            assertThat(Long.parseLong(remaining())).isEqualTo(2L);   // queue RedisRemainingCapacityReader 와 같은 파싱
            assertThat(isMember(USER_A)).isTrue();
            assertThat(gate.isApplicant(DROP_ID, USER_A)).isTrue();
            assertThat(seq()).isEqualTo("1");
        }

        @Test
        @DisplayName("[T4-11] 응모는 queue 토큰 키를 읽기만 한다 — 값·TTL 이 그대로 남는다")
        void tokenIsReadOnly() {
            // Given
            initialize(3);
            issueToken(USER_A);
            String before = redis.opsForValue().get(tokenKey(DROP_ID, USER_A));

            // When
            gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(redis.opsForValue().get(tokenKey(DROP_ID, USER_A))).isEqualTo(before);
            assertThat(redis.getExpire(tokenKey(DROP_ID, USER_A))).isPositive();
        }

        @Test
        @DisplayName("[T4-11] 연속 응모의 seq 는 1, 2, 3 으로 증가하고 isApplicant 는 응모자만 true")
        void seqIncreases() {
            // Given
            initialize(3);

            // When
            GateResult first = applyWithToken(1L);
            GateResult second = applyWithToken(2L);
            GateResult third = applyWithToken(3L);

            // Then
            assertThat(List.of(first.entrySeq(), second.entrySeq(), third.entrySeq())).containsExactly(1, 2, 3);
            assertThat(remaining()).isEqualTo("0");
            assertThat(gate.isApplicant(DROP_ID, 4L)).isFalse();
        }

        @Test
        @DisplayName("[T4-12] meta 없음(초기화 전 드로우) → CLOSED, 아무 키도 만들지 않는다")
        void closedWhenNoMeta() {
            // Given
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(result.entrySeq()).isNull();
            assertThat(redis.hasKey(remainingKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-12] state=CLOSED → CLOSED")
        void closedWhenStateClosed() {
            // Given
            initialize(3);
            redis.opsForHash().put(metaKey(DROP_ID), "state", "CLOSED");
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T4-12] now = openAt - 1ms → CLOSED")
        void closedBeforeOpen() {
            // Given
            initialize(3);
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), OPEN_AT.minusMillis(1));

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(remaining()).isEqualTo("3");
        }

        @Test
        @DisplayName("[T4-12] now = closeAt 정각 → CLOSED")
        void closedAtCloseAt() {
            // Given
            initialize(3);
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), CLOSE_AT);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-12] now = openAt 정각 → OK, now = closeAt - 1ms → OK")
        void okAtBoundaries() {
            // Given
            initialize(3);
            issueToken(USER_A);
            issueToken(USER_B);

            // When
            GateResult atOpen = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), OPEN_AT);
            GateResult justBeforeClose = gate.tryApply(DROP_ID, USER_B, eventId(USER_B), CLOSE_AT.minusMillis(1));

            // Then
            assertThat(atOpen.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(justBeforeClose.outcome()).isEqualTo(ApplyOutcome.OK);
        }

        @Test
        @DisplayName("[T4-12] 판정 순서: 마감이 토큰보다 먼저 — 마감 후 토큰도 없으면 CLOSED")
        void closedPrecedesNoToken() {
            // Given
            initialize(3);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), CLOSE_AT);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
        }

        @Test
        @DisplayName("[T4-13] 토큰 키 없음 → NO_TOKEN, remaining·applicants·seq 변화 없음")
        void noToken() {
            // Given
            initialize(3);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);
            assertThat(result.entrySeq()).isNull();
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-13] 다른 드로우·다른 사용자의 토큰으로는 응모할 수 없다 → NO_TOKEN")
        void tokenOfOtherDropOrUserIsNotAccepted() {
            // Given
            initialize(3);
            redis.opsForValue().set(tokenKey(DROP_ID + 1, USER_A), "1", Duration.ofSeconds(300));
            issueToken(USER_B);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);
            assertThat(remaining()).isEqualTo("3");
        }

        @Test
        @DisplayName("[T4-13] 판정 순서: 토큰이 중복보다 먼저 — 응모자지만 토큰이 만료되면 NO_TOKEN")
        void noTokenPrecedesDuplicate() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            redis.delete(tokenKey(DROP_ID, USER_A));

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);
        }

        @Test
        @DisplayName("[T4-14] 이미 응모한 사용자 → DUPLICATE, remaining·seq 변화 없음")
        void duplicate() {
            // Given
            initialize(3);
            applyWithToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.DUPLICATE);
            assertThat(result.entrySeq()).isNull();
            assertThat(remaining()).isEqualTo("2");
            assertThat(seq()).isEqualTo("1");
            assertThat(redis.opsForSet().size(applicantsKey(DROP_ID))).isEqualTo(1L);
        }

        @Test
        @DisplayName("[T4-14] 판정 순서: 중복이 매진보다 먼저 — 마지막 자리를 차지한 사용자가 다시 응모하면 DUPLICATE")
        void duplicatePrecedesSoldOut() {
            // Given
            initialize(1);
            applyWithToken(USER_A);
            assertThat(remaining()).isEqualTo("0");

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.DUPLICATE);
        }

        @Test
        @DisplayName("[T4-15] remaining=0 → SOLD_OUT, remaining 은 음수가 되지 않고 seq·applicants 변화 없음")
        void soldOut() {
            // Given
            initialize(1);
            applyWithToken(USER_A);

            // When
            GateResult result = applyWithToken(USER_B);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);
            assertThat(result.entrySeq()).isNull();
            assertThat(remaining()).isEqualTo("0");
            assertThat(seq()).isEqualTo("1");
            assertThat(isMember(USER_B)).isFalse();
        }

        @Test
        @DisplayName("[T4-15] 잔여 1 → 마지막 한 명은 OK 이고 remaining=\"0\"")
        void lastSeat() {
            // Given
            initialize(2);
            applyWithToken(USER_A);
            assertThat(remaining()).isEqualTo("1");

            // When
            GateResult result = applyWithToken(USER_B);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(result.entrySeq()).isEqualTo(2);
            assertThat(remaining()).isEqualTo("0");
        }

        @Test
        @DisplayName("[T4-15] 정원 0 으로 초기화된 드로우 → 첫 응모부터 SOLD_OUT")
        void soldOutFromZeroCapacity() {
            // Given
            initialize(0);

            // When
            GateResult result = applyWithToken(USER_A);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);
            assertThat(remaining()).isEqualTo("0");
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @ParameterizedTest(name = "state={0}")
        @ValueSource(strings = {"DRAWN", "open", "OPEN ", ""})
        @DisplayName("[T4-12] state 가 정확히 OPEN 이 아니면(다른 상태·소문자·공백·빈 값) 시각과 무관하게 CLOSED")
        void closedWhenStateIsNotExactlyOpen(String state) {
            // Given
            initialize(3);
            redis.opsForHash().put(metaKey(DROP_ID), "state", state);
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T4-13] 토큰 TTL 만료 직후 → NO_TOKEN, remaining·applicants·seq 변화 없음")
        void noTokenRightAfterTtlExpiry() {
            // Given
            initialize(3);
            redis.opsForValue().set(tokenKey(DROP_ID, USER_A), String.valueOf(NOW.toEpochMilli()),
                    Duration.ofMillis(50));
            await().atMost(Duration.ofSeconds(2))
                    .until(() -> !Boolean.TRUE.equals(redis.hasKey(tokenKey(DROP_ID, USER_A))));

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-15] meta 는 있고 remaining 키만 없음 → SOLD_OUT, remaining 키를 만들지 않는다(음수 \"-1\" 방지)")
        void soldOutWhenRemainingKeyMissing() {
            // Given
            initialize(3);
            redis.delete(remainingKey(DROP_ID));

            // When
            GateResult result = applyWithToken(USER_A);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);
            assertThat(redis.hasKey(remainingKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-14] 거절(DUPLICATE·SOLD_OUT·NO_TOKEN·CLOSED)은 seq 를 올리지 않아 다음 OK 는 연속 번호를 받는다")
        void rejectionsDoNotConsumeSeq() {
            // Given
            initialize(2);
            applyWithToken(USER_A);                                   // seq=1
            gate.tryApply(DROP_ID, USER_A, eventId(USER_A), NOW);                     // DUPLICATE
            gate.tryApply(DROP_ID, USER_B, eventId(USER_B), NOW);                     // NO_TOKEN
            issueToken(USER_B);
            gate.tryApply(DROP_ID, USER_B, eventId(USER_B), CLOSE_AT);                // CLOSED

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_B, eventId(USER_B), NOW);

            // Then
            assertThat(result.entrySeq()).isEqualTo(2);
            assertThat(applyWithToken(3L).outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);
            assertThat(seq()).isEqualTo("2");
        }

        @Test
        @DisplayName("[T4-11] 드로우별 키가 분리된다 — 다른 드로우의 응모는 이 드로우 remaining·seq 에 영향 없음")
        void dropsAreIsolated() {
            // Given
            long otherDrop = DROP_ID + 1;
            initialize(3);
            gate.initialize(otherDrop, 3, OPEN_AT, CLOSE_AT);
            redis.opsForValue().set(tokenKey(otherDrop, USER_A), "1", Duration.ofSeconds(300));

            // When
            GateResult other = gate.tryApply(otherDrop, USER_A, eventId(USER_A), NOW);
            GateResult mine = applyWithToken(USER_A);

            // Then
            assertThat(other.entrySeq()).isEqualTo(1);
            assertThat(mine.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(mine.entrySeq()).isEqualTo(1);
            assertThat(remaining()).isEqualTo("2");
            assertThat(redis.opsForValue().get(remainingKey(otherDrop))).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("보상")
    class Compensate {

        @Test
        @DisplayName("[T4-16] 응모 1건 후 같은 eventId 로 compensate 2번 → COMPENSATED·NOT_OURS, remaining 은 한 번만 +1, applicants·requests 에서 제거, seq 유지")
        void compensateIsIdempotent() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            assertThat(remaining()).isEqualTo("2");

            // When
            CompensationResult first = gate.compensate(DROP_ID, USER_A, eventId(USER_A));
            CompensationResult second = gate.compensate(DROP_ID, USER_A, eventId(USER_A));

            // Then
            assertThat(first).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(second).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(request(USER_A)).isNull();
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(gate.isApplicant(DROP_ID, USER_A)).isFalse();
            assertThat(seq()).isEqualTo("1");
        }

        @Test
        @DisplayName("[T4-16] 응모자가 아닌 사용자 보상 → NOT_OURS, 아무 변화 없음 (정원 초과 복구 방지)")
        void compensateNonApplicantIsNoop() {
            // Given
            initialize(3);
            applyWithToken(USER_A);

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_B, eventId(USER_B));

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("2");
            assertThat(requests()).containsOnlyKeys(String.valueOf(USER_A));
            assertThat(isMember(USER_A)).isTrue();
        }

        @Test
        @DisplayName("[T4-16] 매진 상태에서 보상하면 자리가 하나 생겨 다른 사용자가 OK")
        void compensateReopensSeat() {
            // Given
            initialize(1);
            applyWithToken(USER_A);
            assertThat(applyWithToken(USER_B).outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);

            // When
            gate.compensate(DROP_ID, USER_A, eventId(USER_A));
            GateResult retry = gate.tryApply(DROP_ID, USER_B, eventId(USER_B), NOW);

            // Then
            assertThat(retry.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(remaining()).isEqualTo("0");
        }

        @Test
        @DisplayName("[T4-17] A 응모(seq=1) → 보상 → B 응모 시 B 의 seq=2 (번호 재사용 없음)")
        void seqIsNotReused() {
            // Given
            initialize(3);
            GateResult a = applyWithToken(USER_A);
            assertThat(a.entrySeq()).isEqualTo(1);
            gate.compensate(DROP_ID, USER_A, eventId(USER_A));

            // When
            GateResult b = applyWithToken(USER_B);

            // Then
            assertThat(b.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(b.entrySeq()).isEqualTo(2);
            assertThat(seq()).isEqualTo("2");
        }

        @Test
        @DisplayName("[T4-17] 보상된 사용자가 다시 응모하면 DUPLICATE 가 아니라 OK 이고 새 번호(seq=2)를 받는다")
        void compensatedUserCanReapplyWithNewSeq() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            gate.compensate(DROP_ID, USER_A, eventId(USER_A));

            // When — 재응모는 새 요청 ID
            GateResult again = gate.tryApply(DROP_ID, USER_A, "e-retry", NOW);

            // Then
            assertThat(again.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(again.entrySeq()).isEqualTo(2);
            assertThat(remaining()).isEqualTo("2");
            assertThat(request(USER_A)).isEqualTo("e-retry");
        }

        @Test
        @DisplayName("[T4-17] 보상 후에도 seq 는 유지되어 다음 응모는 이어지는 번호를 받는다 (A=1, B=2, A 보상, C=3)")
        void seqKeepsIncreasingAfterCompensation() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            applyWithToken(USER_B);
            gate.compensate(DROP_ID, USER_A, eventId(USER_A));
            assertThat(seq()).isEqualTo("2");

            // When
            GateResult c = applyWithToken(3L);

            // Then
            assertThat(c.entrySeq()).isEqualTo(3);
            assertThat(remaining()).isEqualTo("1");
            assertThat(redis.opsForSet().members(applicantsKey(DROP_ID))).containsExactlyInAnyOrder("1002", "3");
        }

        @Test
        @DisplayName("[T4-16] 초기화 전 드로우에 보상 → remaining 키를 만들지 않는다 (queue 가 입장 허용으로 오판 방지)")
        void compensateOnUninitializedDropCreatesNothing() {
            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, eventId(USER_A));

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(redis.hasKey(remainingKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(requestsKey(DROP_ID))).isFalse();
        }
    }

    @Nested
    @DisplayName("요청 ID 기록 (v1.5, E-40)")
    class RequestRecord {

        @Test
        @DisplayName("[T4-36] OK 판정은 requests[userId] = eventId 를 기록한다")
        void okRecordsRequest() {
            // Given
            initialize(3);
            issueToken(USER_A);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, "e1", NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(request(USER_A)).isEqualTo("e1");
            assertThat(requests()).hasSize(1);
            assertThat(redis.getExpire(requestsKey(DROP_ID))).isEqualTo(-1L);   // TTL 은 5단계(E-42)
        }

        @Test
        @DisplayName("[T4-36] DUPLICATE 는 기존 요청 ID(e1)를 덮어쓰지 않는다")
        void duplicateKeepsOriginalRequest() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, "e2", NOW);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.DUPLICATE);
            assertThat(request(USER_A)).isEqualTo("e1");
        }

        @Test
        @DisplayName("[T4-36] CLOSED(meta 없음·마감 시각)·NO_TOKEN·SOLD_OUT 은 requests 를 만들지도 바꾸지도 않는다")
        void rejectionsDoNotTouchRequests() {
            // Given — meta 없음
            issueToken(USER_A);

            // When / Then — CLOSED (초기화 전)
            assertThat(gate.tryApply(DROP_ID, USER_A, "e-closed-nometa", NOW).outcome()).isEqualTo(ApplyOutcome.CLOSED);
            assertThat(redis.hasKey(requestsKey(DROP_ID))).isFalse();

            // Given — 정원 1, A 가 e1 로 응모
            initialize(1);
            applyWithToken(USER_A, "e1");

            // When / Then — CLOSED (closeAt 정각)
            issueToken(USER_B);
            assertThat(gate.tryApply(DROP_ID, USER_B, "e-closed", CLOSE_AT).outcome()).isEqualTo(ApplyOutcome.CLOSED);
            // When / Then — NO_TOKEN
            assertThat(gate.tryApply(DROP_ID, 3L, "e-notoken", NOW).outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);
            // When / Then — SOLD_OUT
            assertThat(gate.tryApply(DROP_ID, USER_B, "e-soldout", NOW).outcome()).isEqualTo(ApplyOutcome.SOLD_OUT);

            // Then — requests 는 A 의 e1 하나뿐
            assertThat(requests()).containsOnly(Map.entry(String.valueOf(USER_A), "e1"));
        }

        @Test
        @DisplayName("[T4-36] 불변식: requests 에 있는 userId 는 모두 applicants 에도 있다 (응모·보상·재응모를 섞어도)")
        void requestsAreSubsetOfApplicants() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            applyWithToken(USER_B, "e2");
            gate.compensate(DROP_ID, USER_A, "e1");
            applyWithToken(USER_A, "e3");
            gate.compensate(DROP_ID, USER_B, "e2");

            // Then
            assertThat(requests()).containsOnlyKeys(String.valueOf(USER_A));
            assertThat(request(USER_A)).isEqualTo("e3");
            assertThat(redis.opsForSet().members(applicantsKey(DROP_ID)))
                    .containsAll(requests().keySet().stream().map(String::valueOf).toList());
        }
    }

    @Nested
    @DisplayName("저장 확정 claim (v1.5, E-40)")
    class Claim {

        @Test
        @DisplayName("[T4-37] 응모(e1) 후 claimForSave(e1) → true, persisted 에 userId, 다시 호출해도 true (멱등)")
        void claimMatchingRequest() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");

            // When
            boolean first = gate.claimForSave(DROP_ID, USER_A, "e1");
            boolean second = gate.claimForSave(DROP_ID, USER_A, "e1");

            // Then
            assertThat(first).isTrue();
            assertThat(second).isTrue();
            assertThat(isPersisted(USER_A)).isTrue();
            assertThat(redis.opsForSet().size(persistedKey(DROP_ID))).isEqualTo(1L);
            // claim 은 판정 상태를 바꾸지 않는다
            assertThat(isMember(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("e1");
            assertThat(remaining()).isEqualTo("2");
        }

        @Test
        @DisplayName("[T4-37] 응모자가 없으면 claimForSave → false, persisted 를 만들지 않는다")
        void claimWithoutApplicant() {
            // Given
            initialize(3);

            // When
            boolean claimed = gate.claimForSave(DROP_ID, USER_A, "e1");

            // Then
            assertThat(claimed).isFalse();
            assertThat(redis.hasKey(persistedKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-37] 응모(e1) → 보상(e1) → 재응모(e2) 후 claim(e1) 은 false(유령 이벤트), claim(e2) 는 true")
        void claimRejectsPreviousAttempt() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            assertThat(gate.compensate(DROP_ID, USER_A, "e1")).isEqualTo(CompensationResult.COMPENSATED);
            applyWithToken(USER_A, "e2");

            // When
            boolean ghost = gate.claimForSave(DROP_ID, USER_A, "e1");

            // Then
            assertThat(ghost).isFalse();
            assertThat(isPersisted(USER_A)).isFalse();

            // When
            boolean current = gate.claimForSave(DROP_ID, USER_A, "e2");

            // Then
            assertThat(current).isTrue();
            assertThat(isPersisted(USER_A)).isTrue();
        }

        @Test
        @DisplayName("[T4-37] 보상된 응모(e1)의 유령 이벤트 claim(e1) → false (응모자 없음)")
        void claimAfterCompensation() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            gate.compensate(DROP_ID, USER_A, "e1");

            // When
            boolean claimed = gate.claimForSave(DROP_ID, USER_A, "e1");

            // Then
            assertThat(claimed).isFalse();
            assertThat(isPersisted(USER_A)).isFalse();
        }
    }

    @Nested
    @DisplayName("보상 3결과 (v1.5, E-40)")
    class CompensationResults {

        @Test
        @DisplayName("[T4-38] ① 응모(e1) → compensate(e1) = COMPENSATED: remaining +1, applicants·requests 에서 제거, seq 유지")
        void compensated() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(request(USER_A)).isNull();
            assertThat(seq()).isEqualTo("1");
            assertThat(isPersisted(USER_A)).isFalse();
        }

        @Test
        @DisplayName("[T4-38] ② 응모(e1) → compensate(e2) = NOT_OURS: remaining·applicants·requests 변화 없음")
        void notOursForOtherRequest() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e2");

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isMember(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("e1");
        }

        @Test
        @DisplayName("[T4-38] ③ 판정 없이 compensate(e1) = NOT_OURS: 아무 변화 없음 (E-41 판정 미실행 경로)")
        void notOursWithoutApply() {
            // Given
            initialize(3);

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("3");
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(requestsKey(DROP_ID))).isFalse();
        }

        @Test
        @DisplayName("[T4-38] ④ 응모(e1) → claim(e1) → compensate(e1) = ALREADY_PERSISTED: remaining·applicants·requests·persisted 변화 없음")
        void alreadyPersisted() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            assertThat(gate.claimForSave(DROP_ID, USER_A, "e1")).isTrue();

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e1");

            // Then
            assertThat(result).isEqualTo(CompensationResult.ALREADY_PERSISTED);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isMember(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("e1");
            assertThat(isPersisted(USER_A)).isTrue();
            assertThat(seq()).isEqualTo("1");
        }

        @Test
        @DisplayName("[T4-38] ALREADY_PERSISTED 는 eventId 대조보다 먼저다 — 확정 후 다른 eventId 로 보상해도 ALREADY_PERSISTED")
        void alreadyPersistedPrecedesNotOurs() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            gate.claimForSave(DROP_ID, USER_A, "e1");

            // When
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e2");

            // Then
            assertThat(result).isEqualTo(CompensationResult.ALREADY_PERSISTED);
            assertThat(remaining()).isEqualTo("2");
        }

        @Test
        @DisplayName("[T4-38] (경계) 응모(e1) → 보상(e1) → 재응모(e2) 뒤 늦게 온 보상(e1) = NOT_OURS: 재응모 자리(e2)는 그대로, 이후 claim(e1) false·claim(e2) true")
        void lateCompensationOfPreviousAttemptKeepsReapply() {
            // Given — A 가 보상 후 재응모해 e2 로 자리를 잡음
            initialize(3);
            applyWithToken(USER_A, "e1");
            assertThat(gate.compensate(DROP_ID, USER_A, "e1")).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(applyWithToken(USER_A, "e2").outcome()).isEqualTo(ApplyOutcome.OK);

            // When — 이전 시도(e1)의 보상이 한 번 더 도착 (재시도·중복 호출)
            CompensationResult late = gate.compensate(DROP_ID, USER_A, "e1");

            // Then — 재응모 자리를 풀지 않는다 (정원 초과 복구 방지)
            assertThat(late).isEqualTo(CompensationResult.NOT_OURS);
            assertThat(remaining()).isEqualTo("2");
            assertThat(isMember(USER_A)).isTrue();
            assertThat(request(USER_A)).isEqualTo("e2");
            assertThat(gate.claimForSave(DROP_ID, USER_A, "e1")).isFalse();
            assertThat(gate.claimForSave(DROP_ID, USER_A, "e2")).isTrue();
        }

        @Test
        @DisplayName("[T4-37] (경계) claim 은 사용자별이다 — B 의 eventId 로 A 를 claim 하면 false, A 확정 후에도 B 보상은 COMPENSATED")
        void claimAndCompensateArePerUser() {
            // Given
            initialize(3);
            applyWithToken(USER_A, "e1");
            applyWithToken(USER_B, "e2");

            // When
            boolean crossClaim = gate.claimForSave(DROP_ID, USER_A, "e2");
            boolean claimA = gate.claimForSave(DROP_ID, USER_A, "e1");
            CompensationResult compensateB = gate.compensate(DROP_ID, USER_B, "e2");

            // Then — A 의 persisted 가 B 의 보상을 막지 않는다
            assertThat(crossClaim).isFalse();
            assertThat(claimA).isTrue();
            assertThat(compensateB).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(isPersisted(USER_B)).isFalse();
            assertThat(isMember(USER_B)).isFalse();
            assertThat(isMember(USER_A)).isTrue();
            assertThat(remaining()).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("Redis 명령 타임아웃 (v1.5, E-41)")
    class CommandTimeout {

        /** 명령 타임아웃(spring.data.redis.timeout=1s)보다 길게 */
        private static final long PAUSE_MS = 1_500L;

        /** 단언이 중간에 실패해도 pause 가 다음 테스트의 FLUSHALL 을 타임아웃시키지 않게 끝날 때까지 기다린다 */
        @AfterEach
        void waitForPauseEnd() {
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> redis.hasKey("pause-probe") != null);
        }

        @Test
        @DisplayName("[T4-39] CLIENT PAUSE 로 판정 Lua 가 타임아웃되면 GateUnavailableException(원인 DataAccessException), pause 후 Lua 는 실행돼 있고 같은 eventId 보상은 COMPENSATED")
        void timeoutBecomesGateUnavailable() {
            // Given — 응모 가능 상태
            initialize(3);
            issueToken(USER_A);
            // 스크립트 캐시 적재: 캐시가 없으면 pause 중 EVALSHA 가 타임아웃되고 EVAL 재시도가 없어 Lua 가 실행되지 않는다
            assertThat(gate.tryApply(DROP_ID, USER_B, "e-warmup", NOW).outcome()).isEqualTo(ApplyOutcome.NO_TOKEN);

            RedisClient client = RedisClient.create(
                    RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
            try (StatefulRedisConnection<String, String> pauser = client.connect()) {
                pauser.sync().clientPause(PAUSE_MS);                    // CLIENT PAUSE 1500 (기본 ALL)

                // When
                Throwable thrown = catchThrowable(() -> gate.tryApply(DROP_ID, USER_A, "e1", NOW));

                // Then — 포트 예외로 감싸진다
                assertThat(thrown).isInstanceOf(GateUnavailableException.class);
                assertThat(thrown.getCause()).isInstanceOf(DataAccessException.class);
            } finally {
                client.shutdown();
            }

            // Then — pause 가 끝나면 늦게 도착한 판정 Lua 는 실행돼 있다 (pause 중 조회는 타임아웃되므로 예외 무시)
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> isMember(USER_A));
            assertThat(request(USER_A)).isEqualTo("e1");
            assertThat(remaining()).isEqualTo("2");

            // When — 서비스가 같은 eventId 로 보상
            CompensationResult result = gate.compensate(DROP_ID, USER_A, "e1");

            // Then — 원상복구
            assertThat(result).isEqualTo(CompensationResult.COMPENSATED);
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(request(USER_A)).isNull();
        }

        @Test
        @DisplayName("[T4-39] (경계) 저장 확정 claim·보상도 Redis 타임아웃이면 GateUnavailableException(원인 DataAccessException) 으로 감싼다")
        void claimAndCompensateTimeoutBecomeGateUnavailable() {
            // Given — 응모(e1) 완료, 스크립트 캐시 적재 (claim·compensate 각각 한 번 실행)
            initialize(3);
            applyWithToken(USER_A, "e1");
            assertThat(gate.claimForSave(DROP_ID, USER_B, "e-warmup")).isFalse();
            assertThat(gate.compensate(DROP_ID, USER_B, "e-warmup")).isEqualTo(CompensationResult.NOT_OURS);

            RedisClient client = RedisClient.create(
                    RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
            try (StatefulRedisConnection<String, String> pauser = client.connect()) {
                // When — claim 타임아웃
                pauser.sync().clientPause(PAUSE_MS);
                Throwable claimThrown = catchThrowable(() -> gate.claimForSave(DROP_ID, USER_A, "e1"));

                // Then
                assertThat(claimThrown).isInstanceOf(GateUnavailableException.class);
                assertThat(claimThrown.getCause()).isInstanceOf(DataAccessException.class);
            } finally {
                client.shutdown();
            }
            waitForPauseEnd();

            client = RedisClient.create(
                    RedisURI.create(connectionFactory.getHostName(), connectionFactory.getPort()));
            try (StatefulRedisConnection<String, String> pauser = client.connect()) {
                // When — 보상 타임아웃
                pauser.sync().clientPause(PAUSE_MS);
                Throwable compensateThrown = catchThrowable(() -> gate.compensate(DROP_ID, USER_A, "e-other"));

                // Then
                assertThat(compensateThrown).isInstanceOf(GateUnavailableException.class);
                assertThat(compensateThrown.getCause()).isInstanceOf(DataAccessException.class);
            } finally {
                client.shutdown();
            }
        }
    }
}
