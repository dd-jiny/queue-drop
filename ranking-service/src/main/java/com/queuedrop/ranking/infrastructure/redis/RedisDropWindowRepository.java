package com.queuedrop.ranking.infrastructure.redis;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.DropWindow;
import com.queuedrop.ranking.domain.repository.DropWindowRepository;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/** 드로우 마감 시각 저장소 (구현 결정). 값은 epoch ms 문자열 — rank.lua 가 tonumber 로 읽는다 */
@Repository
@RequiredArgsConstructor
class RedisDropWindowRepository implements DropWindowRepository {

    private final StringRedisTemplate redis;

    @Override
    public void save(long dropId, Instant closeAt) {
        call(() -> {
            redis.opsForHash().put(RankingKeys.DROP_CLOSE_AT, String.valueOf(dropId),
                    String.valueOf(closeAt.toEpochMilli()));
            return null;
        });
    }

    /** HMGET. 빈 입력은 Redis 호출 없이 빈 맵, 중복 id 는 한 번만, 없는 id 는 closeAt=null */
    @Override
    public Map<Long, DropWindow> findAll(Collection<Long> dropIds) {
        List<Long> distinct = dropIds.stream().distinct().toList();
        if (distinct.isEmpty()) {
            return Map.of();
        }
        List<Object> fields = distinct.stream().<Object>map(String::valueOf).toList();
        List<Object> values = call(() -> redis.opsForHash().multiGet(RankingKeys.DROP_CLOSE_AT, fields));
        Map<Long, DropWindow> result = new HashMap<>();
        for (int i = 0; i < distinct.size(); i++) {
            Object value = values.get(i);
            Instant closeAt = value == null ? null : Instant.ofEpochMilli(Long.parseLong((String) value));
            result.put(distinct.get(i), new DropWindow(distinct.get(i), closeAt));
        }
        return result;
    }

    /** Redis 예외(DataAccessException 계열)를 포트 예외로 */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new RankingStoreUnavailableException("Redis 드로우 마감 시각 저장소 호출 실패", e);
        }
    }
}
