package com.queuedrop.ranking.infrastructure.redis;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.RankEntry;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import com.queuedrop.ranking.domain.repository.RankingRepository;
import com.queuedrop.ranking.infrastructure.config.RankingProperties;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

/** RankingRepository 의 Redis 구현. 집계는 rank.lua 로 dedupe 와 3개 보드 증가를 원자 실행한다 */
@Repository
@RequiredArgsConstructor
class RedisRankingRepository implements RankingRepository {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> rankScript;          // 빈 이름 = 필드 이름 (RedisScriptConfig)
    private final RankingProperties props;
    private final Clock clock;

    @Override
    public boolean incrementOnce(long dropId, long userId, WeekKey week) {
        List<String> keys = List.of(
                RankingKeys.dedupe(dropId), RankingKeys.DROP_POPULAR, RankingKeys.weekly(week),
                RankingKeys.USER_ALL, RankingKeys.DROP_CLOSE_AT);
        Long result = call(() -> redis.execute(rankScript, keys,
                String.valueOf(dropId), String.valueOf(userId),
                String.valueOf(props.weeklyTtl().toSeconds()),
                String.valueOf(props.dedupeGrace().toMillis()),
                String.valueOf(props.dedupeFallbackTtl().toMillis()),
                String.valueOf(props.dedupeMinTtl().toMillis()),
                String.valueOf(clock.millis())));
        return Long.valueOf(1L).equals(result);
    }

    @Override
    public List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count) {
        String key = RankingKeys.board(board, week);
        Set<TypedTuple<String>> tuples = call(() -> redis.opsForZSet()
                .reverseRangeWithScores(key, offset, offset + count - 1));
        if (tuples == null || tuples.isEmpty()) {
            return List.of();
        }
        // tuples 는 점수 내림차순. rank = offset + index + 1
        List<RankEntry> entries = new ArrayList<>(tuples.size());
        long rank = offset;
        for (TypedTuple<String> tuple : tuples) {
            entries.add(new RankEntry(++rank, Long.parseLong(tuple.getValue()), tuple.getScore().longValue()));
        }
        return entries;
    }

    @Override
    public Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId) {
        String key = RankingKeys.board(board, week);
        String member = String.valueOf(memberId);
        Long rank = call(() -> redis.opsForZSet().reverseRank(key, member));
        if (rank == null) {
            return Optional.empty();
        }
        Double score = call(() -> redis.opsForZSet().score(key, member));
        if (score == null) {
            return Optional.empty();          // 두 호출 사이에 제거된 경우
        }
        return Optional.of(new RankEntry(rank + 1, memberId, score.longValue()));
    }

    @Override
    public void removeDrops(Collection<Long> dropIds) {
        if (dropIds.isEmpty()) {
            return;                           // ZREM 0인자 오류 방지, Redis 호출 없음
        }
        Object[] members = dropIds.stream().map(String::valueOf).toArray();
        call(() -> redis.opsForZSet().remove(RankingKeys.DROP_POPULAR, members));
    }

    /** Redis 예외(연결 실패·명령 타임아웃 = QueryTimeoutException 등 DataAccessException 계열)를 포트 예외로 */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new RankingStoreUnavailableException("Redis 랭킹 저장소 호출 실패", e);
        }
    }
}
