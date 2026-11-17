package com.queuedrop.ranking.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.ranking.domain.model.RankEntry;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import com.queuedrop.ranking.infrastructure.config.RankingProperties;
import com.queuedrop.ranking.infrastructure.config.RedisScriptConfig;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Redis 랭킹 저장소 + rank.lua 통합 테스트 (phase-7-ranking.md 4장 T7-13 ~ T7-17, T7-30(Redis)).
 * 키 이름은 3.0 / 3.3 문자열을 테스트가 독립적으로 고정한다 (RankingKeys 와 교차 검증).
 * Kafka 컨테이너 없이 Redis 슬라이스만 띄운다 (4장 테스트 격리 규칙).
 * Clock 은 실제 시각 — PEXPIREAT 가 Redis 서버 시각 기준이라 고정 과거 시각이면 키가 바로 만료된다.
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisRankingRepository.class,
        RedisDropWindowRepository.class, RedisRankingRepositoryTest.TestClock.class})
class RedisRankingRepositoryTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class TestClock {
        @Bean
        @Primary
        Clock testClock() {
            return Clock.offset(Clock.systemUTC(), Duration.ZERO);
        }
    }

    private static final long DROP_A = 10L;
    private static final long DROP_B = 20L;
    private static final long USER_1 = 1001L;
    private static final long USER_2 = 1002L;
    private static final WeekKey W47 = WeekKey.parse("2026-W47");
    private static final WeekKey W48 = WeekKey.parse("2026-W48");

    private static final Duration WEEKLY_TTL = Duration.ofDays(35);
    private static final Duration DEDUPE_GRACE = Duration.ofDays(7);
    private static final Duration DEDUPE_FALLBACK = Duration.ofDays(30);
    private static final Duration DEDUPE_MIN = Duration.ofDays(1);
    /** 앱 시각(호스트)과 Redis 서버 시각(컨테이너) 차이 + 실행 시간 여유 */
    private static final long TOLERANCE_MS = 60_000L;

    @Autowired private RedisRankingRepository repository;
    @Autowired private RedisDropWindowRepository dropWindowRepository;
    @Autowired private StringRedisTemplate redis;
    @Autowired private RankingProperties props;
    @Autowired private RedisScript<Long> rankScript;
    @Autowired private Clock clock;
    @Autowired private LettuceConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
    }

    // ---- 키 (3.0) ----
    private static final String POPULAR = "rank:drop:popular";
    private static final String USER_ALL = "rank:user:all";
    private static final String CLOSE_AT = "rank:drop:closeAt";
    private static String weeklyKey(WeekKey week) { return "rank:user:weekly:" + week.value(); }
    private static String dedupeKey(long dropId) { return "rank:dedupe:" + dropId; }

    // ---- 도우미 ----
    private Double score(String key, long member) {
        return redis.opsForZSet().score(key, String.valueOf(member));
    }

    private long pttl(String key) {
        return redis.getExpire(key, TimeUnit.MILLISECONDS);
    }

    private void zadd(String key, long member, double score) {
        redis.opsForZSet().add(key, String.valueOf(member), score);
    }

    private static void assertNear(long actualMs, long expectedMs) {
        assertThat(actualMs).isBetween(expectedMs - TOLERANCE_MS, expectedMs + TOLERANCE_MS);
    }

    @Test
    @DisplayName("[T7-13] (경계) RankingKeys 는 3.0 키 문자열을 만들고, USER_WEEKLY 에 week 가 없으면 NPE")
    void keysMatchDesign() {
        // Given / When / Then
        assertThat(RankingKeys.DROP_POPULAR).isEqualTo(POPULAR);
        assertThat(RankingKeys.USER_ALL).isEqualTo(USER_ALL);
        assertThat(RankingKeys.DROP_INFO).isEqualTo("rank:drop:info");
        assertThat(RankingKeys.USER_INFO).isEqualTo("rank:user:info");
        assertThat(RankingKeys.DROP_CLOSE_AT).isEqualTo(CLOSE_AT);
        assertThat(RankingKeys.weekly(W47)).isEqualTo("rank:user:weekly:2026-W47");
        assertThat(RankingKeys.dedupe(DROP_A)).isEqualTo("rank:dedupe:10");
        assertThat(RankingKeys.board(RankingBoard.DROP_POPULAR, null)).isEqualTo(POPULAR);
        assertThat(RankingKeys.board(RankingBoard.USER_ALL, null)).isEqualTo(USER_ALL);
        assertThat(RankingKeys.board(RankingBoard.USER_WEEKLY, W48)).isEqualTo("rank:user:weekly:2026-W48");
        assertThatThrownBy(() -> RankingKeys.board(RankingBoard.USER_WEEKLY, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("[T7-15] (경계) ranking.* 기본값이 application.yml 에서 바인딩된다 (35d / 7d / 30d / 1d)")
    void propertiesBound() {
        // Given / When / Then
        assertThat(props.weeklyTtl()).isEqualTo(WEEKLY_TTL);
        assertThat(props.dedupeGrace()).isEqualTo(DEDUPE_GRACE);
        assertThat(props.dedupeFallbackTtl()).isEqualTo(DEDUPE_FALLBACK);
        assertThat(props.dedupeMinTtl()).isEqualTo(DEDUPE_MIN);
    }

    @Nested
    @DisplayName("incrementOnce (rank.lua)")
    class IncrementOnce {

        @Test
        @DisplayName("[T7-13] 같은 (dropId, userId) 로 3번 호출하면 첫 호출만 true, 3개 보드 점수 모두 1")
        void dedupe() {
            // When
            boolean first = repository.incrementOnce(DROP_A, USER_1, W47);
            boolean second = repository.incrementOnce(DROP_A, USER_1, W47);
            boolean third = repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            assertThat(List.of(first, second, third)).containsExactly(true, false, false);
            assertThat(score(POPULAR, DROP_A)).isEqualTo(1.0);
            assertThat(score(weeklyKey(W47), USER_1)).isEqualTo(1.0);
            assertThat(score(USER_ALL, USER_1)).isEqualTo(1.0);
            assertThat(redis.opsForSet().members(dedupeKey(DROP_A))).containsExactly(String.valueOf(USER_1));
        }

        @Test
        @DisplayName("[T7-13] (경계) dedupe 는 (dropId, userId) 기준 — 같은 응모를 다른 주차로 다시 보내도 false, 새 주간 키를 만들지 않음")
        void dedupeIgnoresWeek() {
            // Given
            assertThat(repository.incrementOnce(DROP_A, USER_1, W47)).isTrue();

            // When
            boolean again = repository.incrementOnce(DROP_A, USER_1, W48);

            // Then
            assertThat(again).isFalse();
            assertThat(redis.hasKey(weeklyKey(W48))).isFalse();
            assertThat(score(USER_ALL, USER_1)).isEqualTo(1.0);
        }

        @Test
        @DisplayName("[T7-13] (경계) 같은 사용자가 다른 드로우 2개에 응모 → 드로우별 popular 1, weekly·all 은 2")
        void sameUserTwoDrops() {
            // When
            assertThat(repository.incrementOnce(DROP_A, USER_1, W47)).isTrue();
            assertThat(repository.incrementOnce(DROP_B, USER_1, W47)).isTrue();

            // Then
            assertThat(score(POPULAR, DROP_A)).isEqualTo(1.0);
            assertThat(score(POPULAR, DROP_B)).isEqualTo(1.0);
            assertThat(score(weeklyKey(W47), USER_1)).isEqualTo(2.0);
            assertThat(score(USER_ALL, USER_1)).isEqualTo(2.0);
        }

        @Test
        @DisplayName("[T7-14] 같은 드로우에 사용자 2명이 각 1회 → popular=2, 각 사용자 weekly=1, all=1")
        void twoUsersSameDrop() {
            // When
            assertThat(repository.incrementOnce(DROP_A, USER_1, W47)).isTrue();
            assertThat(repository.incrementOnce(DROP_A, USER_2, W47)).isTrue();

            // Then
            assertThat(score(POPULAR, DROP_A)).isEqualTo(2.0);
            assertThat(score(weeklyKey(W47), USER_1)).isEqualTo(1.0);
            assertThat(score(weeklyKey(W47), USER_2)).isEqualTo(1.0);
            assertThat(score(USER_ALL, USER_1)).isEqualTo(1.0);
            assertThat(score(USER_ALL, USER_2)).isEqualTo(1.0);
            assertThat(redis.opsForSet().size(dedupeKey(DROP_A))).isEqualTo(2L);
        }

        @Test
        @DisplayName("[T7-15] 첫 기록 후 weekly 키 TTL ≈ 35일, popular·all 은 TTL 없음")
        void weeklyTtlOnFirstRecord() {
            // When
            repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            long ttlSeconds = redis.getExpire(weeklyKey(W47), TimeUnit.SECONDS);
            assertThat(ttlSeconds).isBetween(WEEKLY_TTL.toSeconds() - 60, WEEKLY_TTL.toSeconds());
            assertThat(redis.getExpire(POPULAR, TimeUnit.SECONDS)).isEqualTo(-1L);
            assertThat(redis.getExpire(USER_ALL, TimeUnit.SECONDS)).isEqualTo(-1L);
        }

        @Test
        @DisplayName("[T7-15] 두 번째 기록은 weekly 키 TTL 을 연장하지 않는다 (남은 TTL 1000초 → 그대로 1000초 이하)")
        void weeklyTtlNotExtended() {
            // Given — 첫 기록 후 남은 TTL 을 1000초로 줄여 둔다 (시간 경과를 흉내)
            repository.incrementOnce(DROP_A, USER_1, W47);
            redis.expire(weeklyKey(W47), Duration.ofSeconds(1000));

            // When — 다른 사용자·다른 드로우의 기록
            repository.incrementOnce(DROP_A, USER_2, W47);
            repository.incrementOnce(DROP_B, USER_1, W47);

            // Then
            assertThat(redis.getExpire(weeklyKey(W47), TimeUnit.SECONDS)).isBetween(1L, 1000L);
            assertThat(score(weeklyKey(W47), USER_1)).isEqualTo(2.0);
        }

        @Test
        @DisplayName("[T7-15] (경계) weekly 키가 TTL 없이 존재하면(-1) 다음 기록 때 35일 TTL 을 설정한다")
        void weeklyTtlSetWhenMissing() {
            // Given — TTL 없는 주간 키 (수동 조작·과거 버그 등)
            zadd(weeklyKey(W47), USER_2, 3);
            assertThat(redis.getExpire(weeklyKey(W47), TimeUnit.SECONDS)).isEqualTo(-1L);

            // When
            repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            assertThat(redis.getExpire(weeklyKey(W47), TimeUnit.SECONDS))
                    .isBetween(WEEKLY_TTL.toSeconds() - 60, WEEKLY_TTL.toSeconds());
            assertThat(score(weeklyKey(W47), USER_2)).isEqualTo(3.0);
        }

        @Test
        @DisplayName("[T7-16] closeAt 을 알면 dedupe 키는 closeAt + 7일에 만료된다 (PTTL 범위)")
        void dedupeTtlFromCloseAt() {
            // Given — 2시간 뒤 마감
            Instant closeAt = clock.instant().plus(Duration.ofHours(2));
            redis.opsForHash().put(CLOSE_AT, String.valueOf(DROP_A), String.valueOf(closeAt.toEpochMilli()));

            // When
            repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            long expireAt = closeAt.plus(DEDUPE_GRACE).toEpochMilli();
            assertNear(pttl(dedupeKey(DROP_A)), expireAt - System.currentTimeMillis());
        }

        @Test
        @DisplayName("[T7-16] closeAt 을 모르면(drop.created 미수신) dedupe 키는 now + 30일에 만료된다")
        void dedupeTtlFallback() {
            // When
            repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            assertNear(pttl(dedupeKey(DROP_A)), DEDUPE_FALLBACK.toMillis());
        }

        @Test
        @DisplayName("[T7-16] closeAt 이 10일 전이면(closeAt+7일이 이미 지남) 최소 TTL now + 1일을 보장한다")
        void dedupeTtlMinimum() {
            // Given
            Instant closeAt = clock.instant().minus(Duration.ofDays(10));
            redis.opsForHash().put(CLOSE_AT, String.valueOf(DROP_A), String.valueOf(closeAt.toEpochMilli()));

            // When
            boolean recorded = repository.incrementOnce(DROP_A, USER_1, W47);

            // Then — 키가 즉시 삭제되지 않고 집계·dedupe 가 남는다
            assertThat(recorded).isTrue();
            assertNear(pttl(dedupeKey(DROP_A)), DEDUPE_MIN.toMillis());
            assertThat(repository.incrementOnce(DROP_A, USER_1, W47)).isFalse();
        }

        @Test
        @DisplayName("[T7-16] (경계) dedupe TTL 은 첫 기록 때만 정한다 — 이후 closeAt 이 들어와도 두 번째 사용자 기록이 TTL 을 바꾸지 않음")
        void dedupeTtlSetOnce() {
            // Given — closeAt 미상으로 첫 기록 (30일)
            repository.incrementOnce(DROP_A, USER_1, W47);
            redis.opsForHash().put(CLOSE_AT, String.valueOf(DROP_A),
                    String.valueOf(clock.instant().plus(Duration.ofHours(2)).toEpochMilli()));

            // When
            repository.incrementOnce(DROP_A, USER_2, W47);

            // Then
            assertNear(pttl(dedupeKey(DROP_A)), DEDUPE_FALLBACK.toMillis());
        }

        @Test
        @DisplayName("[T7-16] (경계) DropWindowRepository.save 로 저장한 closeAt 형식(epoch ms)을 rank.lua 가 그대로 읽는다")
        void luaReadsCloseAtSavedByDropWindowRepository() {
            // Given
            Instant closeAt = clock.instant().plus(Duration.ofDays(3));
            dropWindowRepository.save(DROP_A, closeAt);

            // When
            repository.incrementOnce(DROP_A, USER_1, W47);

            // Then
            long expireAt = closeAt.plus(DEDUPE_GRACE).toEpochMilli();
            assertNear(pttl(dedupeKey(DROP_A)), expireAt - System.currentTimeMillis());
        }
    }

    @Nested
    @DisplayName("top / rankOf")
    class Read {

        @Test
        @DisplayName("[T7-17] 동점 2명은 ZREVRANGE 사전 역순(\"9\" > \"10\")으로, 순번 1·2 를 서로 다르게 받는다")
        void tieBreakReverseLexicographic() {
            // Given — 3.5 예시: 점수 5 인 userId "10", "9"
            zadd(USER_ALL, 10L, 5);
            zadd(USER_ALL, 9L, 5);
            zadd(USER_ALL, 20L, 3);

            // When
            List<RankEntry> top = repository.top(RankingBoard.USER_ALL, null, 0, 10);

            // Then
            assertThat(top).containsExactly(
                    new RankEntry(1, 9L, 5), new RankEntry(2, 10L, 5), new RankEntry(3, 20L, 3));
            assertThat(repository.rankOf(RankingBoard.USER_ALL, null, 10L)).contains(new RankEntry(2, 10L, 5));
            assertThat(repository.rankOf(RankingBoard.USER_ALL, null, 9L)).contains(new RankEntry(1, 9L, 5));
        }

        @Test
        @DisplayName("[T7-17] (경계) offset·count 구간 [offset, offset+count) 와 순번 = offset + index + 1, 범위 밖·빈 보드는 빈 목록")
        void topWindow() {
            // Given — 점수 5..1
            for (long id = 1; id <= 5; id++) {
                zadd(POPULAR, id, 6 - id);
            }

            // When / Then
            assertThat(repository.top(RankingBoard.DROP_POPULAR, null, 2, 2))
                    .containsExactly(new RankEntry(3, 3L, 3), new RankEntry(4, 4L, 2));
            assertThat(repository.top(RankingBoard.DROP_POPULAR, null, 4, 10))
                    .containsExactly(new RankEntry(5, 5L, 1));
            assertThat(repository.top(RankingBoard.DROP_POPULAR, null, 5, 10)).isEmpty();
            assertThat(repository.top(RankingBoard.USER_ALL, null, 0, 10)).isEmpty();
        }

        @Test
        @DisplayName("[T7-17] (경계) USER_WEEKLY 는 week 의 키만 읽는다 — 다른 주차는 빈 목록·empty")
        void weeklyReadsWeekKey() {
            // Given
            repository.incrementOnce(DROP_A, USER_1, W47);

            // When / Then
            assertThat(repository.top(RankingBoard.USER_WEEKLY, W47, 0, 10))
                    .containsExactly(new RankEntry(1, USER_1, 1));
            assertThat(repository.top(RankingBoard.USER_WEEKLY, W48, 0, 10)).isEmpty();
            assertThat(repository.rankOf(RankingBoard.USER_WEEKLY, W47, USER_1)).contains(new RankEntry(1, USER_1, 1));
            assertThat(repository.rankOf(RankingBoard.USER_WEEKLY, W48, USER_1)).isEmpty();
        }

        @Test
        @DisplayName("[T7-17] (경계) 보드에 없는 member 는 rankOf empty")
        void rankOfMissing() {
            // Given
            zadd(USER_ALL, USER_1, 2);

            // When
            Optional<RankEntry> missing = repository.rankOf(RankingBoard.USER_ALL, null, USER_2);

            // Then
            assertThat(missing).isEmpty();
            assertThat(repository.rankOf(RankingBoard.DROP_POPULAR, null, DROP_A)).isEmpty();
        }
    }

    @Nested
    @DisplayName("removeDrops")
    class RemoveDrops {

        @Test
        @DisplayName("[T7-30] removeDrops 후 인기 보드에서 빠지고, 없는 id 는 무시한다 (다른 보드는 그대로)")
        void removesAndIgnoresMissing() {
            // Given
            zadd(POPULAR, 1L, 5);
            zadd(POPULAR, 2L, 4);
            zadd(POPULAR, 3L, 3);
            zadd(USER_ALL, 1L, 7);

            // When
            repository.removeDrops(List.of(1L, 3L, 999L));

            // Then
            assertThat(repository.top(RankingBoard.DROP_POPULAR, null, 0, 10))
                    .containsExactly(new RankEntry(1, 2L, 4));
            assertThat(score(USER_ALL, 1L)).isEqualTo(7.0);

            // When — 같은 요청을 다시 (멱등)
            repository.removeDrops(List.of(1L, 3L));

            // Then
            assertThat(redis.opsForZSet().size(POPULAR)).isEqualTo(1L);
        }

        @Test
        @DisplayName("[T7-30] (경계) 빈 목록이면 오류 없이 아무것도 하지 않는다 (ZREM 0인자 오류 없음)")
        void emptyIsNoop() {
            // Given
            zadd(POPULAR, 1L, 5);

            // When
            repository.removeDrops(List.of());

            // Then
            assertThat(redis.opsForZSet().size(POPULAR)).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("Redis 장애 (E-03)")
    class RedisFailure {

        /** 단언이 중간에 실패해도 pause 가 다음 테스트의 FLUSHALL 을 막지 않게 끝날 때까지 기다린다 */
        @AfterEach
        void waitForPauseEnd() {
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> {
                        redis.opsForValue().set("pause-probe", "1");
                        return true;
                    });
        }

        @Test
        @DisplayName("[T7-27] (경계, Redis) 잘못된 포트면 incrementOnce·top·rankOf·removeDrops 모두 RankingStoreUnavailableException(원인 DataAccessException)")
        void unreachableRedis() {
            // Given
            LettuceConnectionFactory badFactory = RedisTestSupport.unreachableFactory(connectionFactory.getHostName());
            try {
                RedisRankingRepository bad = new RedisRankingRepository(
                        new StringRedisTemplate(badFactory), rankScript, props, clock);
                Map<String, Consumer<RedisRankingRepository>> calls = Map.of(
                        "incrementOnce", r -> r.incrementOnce(DROP_A, USER_1, W47),
                        "top", r -> r.top(RankingBoard.DROP_POPULAR, null, 0, 10),
                        "topWeekly", r -> r.top(RankingBoard.USER_WEEKLY, W47, 0, 10),
                        "rankOf", r -> r.rankOf(RankingBoard.USER_ALL, null, USER_1),
                        "removeDrops", r -> r.removeDrops(List.of(DROP_A)));

                // When / Then
                RedisTestSupport.assertAllStoreUnavailable(bad, calls);

                // Then — 빈 목록 removeDrops 는 Redis 를 부르지 않으므로 장애여도 예외 없음
                assertThat(catchThrowable(() -> bad.removeDrops(List.of()))).isNull();
            } finally {
                badFactory.destroy();
            }
        }

        @Test
        @DisplayName("[T7-27] (경계, Redis) CLIENT PAUSE WRITE 로 rank.lua 가 1초 타임아웃되면 RankingStoreUnavailableException, pause 후 Lua 는 실행돼 있고 재시도는 false (이중 집계 없음)")
        void timeoutIsAmbiguousButIdempotent() {
            // Given — 스크립트 캐시 적재 (캐시가 없으면 pause 중 EVALSHA 가 타임아웃되고 EVAL 재시도가 없다)
            repository.incrementOnce(DROP_B, USER_2, W47);

            // When
            Throwable[] thrown = new Throwable[1];
            RedisTestSupport.withWritePause(connectionFactory,
                    () -> thrown[0] = catchThrowable(() -> repository.incrementOnce(DROP_A, USER_1, W47)));

            // Then — 포트 예외로 감싸진다 (spring.data.redis.timeout=1s 가 적용돼야 pause 1.5초 안에 실패)
            RedisTestSupport.assertStoreUnavailable("incrementOnce", thrown[0]);

            // Then — 늦게 도착한 Lua 는 실행돼 있다
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                    .until(() -> Double.valueOf(1.0).equals(score(POPULAR, DROP_A)));

            // When — 리스너 재시도
            boolean retried = repository.incrementOnce(DROP_A, USER_1, W47);

            // Then — dedupe 로 멱등
            assertThat(retried).isFalse();
            assertThat(score(POPULAR, DROP_A)).isEqualTo(1.0);
            assertThat(score(USER_ALL, USER_1)).isEqualTo(1.0);
        }
    }
}
