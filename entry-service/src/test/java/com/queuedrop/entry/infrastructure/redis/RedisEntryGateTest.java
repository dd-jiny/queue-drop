package com.queuedrop.entry.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import com.queuedrop.entry.infrastructure.config.RedisScriptConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 응모 Lua 게이트 통합 테스트 (단계 문서 4장 T4-11~18).
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

    private GateResult applyWithToken(long userId) {
        issueToken(userId);
        return gate.tryApply(DROP_ID, userId, NOW);
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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, OPEN_AT.minusMillis(1));

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, CLOSE_AT);

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
            GateResult atOpen = gate.tryApply(DROP_ID, USER_A, OPEN_AT);
            GateResult justBeforeClose = gate.tryApply(DROP_ID, USER_B, CLOSE_AT.minusMillis(1));

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, CLOSE_AT);

            // Then
            assertThat(result.outcome()).isEqualTo(ApplyOutcome.CLOSED);
        }

        @Test
        @DisplayName("[T4-13] 토큰 키 없음 → NO_TOKEN, remaining·applicants·seq 변화 없음")
        void noToken() {
            // Given
            initialize(3);

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            GateResult result = gate.tryApply(DROP_ID, USER_A, NOW);

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
            gate.tryApply(DROP_ID, USER_A, NOW);                     // DUPLICATE
            gate.tryApply(DROP_ID, USER_B, NOW);                     // NO_TOKEN
            issueToken(USER_B);
            gate.tryApply(DROP_ID, USER_B, CLOSE_AT);                // CLOSED

            // When
            GateResult result = gate.tryApply(DROP_ID, USER_B, NOW);

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
            GateResult other = gate.tryApply(otherDrop, USER_A, NOW);
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
        @DisplayName("[T4-16] 응모 1건 후 compensate 2번 → remaining 은 한 번만 +1, applicants 에서 제거, seq 유지")
        void compensateIsIdempotent() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            assertThat(remaining()).isEqualTo("2");

            // When
            gate.compensate(DROP_ID, USER_A);
            gate.compensate(DROP_ID, USER_A);

            // Then
            assertThat(remaining()).isEqualTo("3");
            assertThat(isMember(USER_A)).isFalse();
            assertThat(gate.isApplicant(DROP_ID, USER_A)).isFalse();
            assertThat(seq()).isEqualTo("1");
        }

        @Test
        @DisplayName("[T4-16] 응모자가 아닌 사용자 보상 → 아무 변화 없음 (정원 초과 복구 방지)")
        void compensateNonApplicantIsNoop() {
            // Given
            initialize(3);
            applyWithToken(USER_A);

            // When
            gate.compensate(DROP_ID, USER_B);

            // Then
            assertThat(remaining()).isEqualTo("2");
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
            gate.compensate(DROP_ID, USER_A);
            GateResult retry = gate.tryApply(DROP_ID, USER_B, NOW);

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
            gate.compensate(DROP_ID, USER_A);

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
            gate.compensate(DROP_ID, USER_A);

            // When
            GateResult again = gate.tryApply(DROP_ID, USER_A, NOW);

            // Then
            assertThat(again.outcome()).isEqualTo(ApplyOutcome.OK);
            assertThat(again.entrySeq()).isEqualTo(2);
            assertThat(remaining()).isEqualTo("2");
        }
            @Test
        @DisplayName("[T4-17] 보상 후에도 seq 는 유지되어 다음 응모는 이어지는 번호를 받는다 (A=1, B=2, A 보상, C=3)")
        void seqKeepsIncreasingAfterCompensation() {
            // Given
            initialize(3);
            applyWithToken(USER_A);
            applyWithToken(USER_B);
            gate.compensate(DROP_ID, USER_A);
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
            gate.compensate(DROP_ID, USER_A);

            // Then
            assertThat(redis.hasKey(remainingKey(DROP_ID))).isFalse();
            assertThat(redis.hasKey(applicantsKey(DROP_ID))).isFalse();
        }
    }
}
