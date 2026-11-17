package com.queuedrop.ranking.infrastructure.redis;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.repository.DisplayNameRepository;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/** 표시 정보(드로우 제목·닉네임) Redis Hash 저장소. HSET 덮어쓰기라 재전달에 멱등 */
@Repository
@RequiredArgsConstructor
class RedisDisplayNameRepository implements DisplayNameRepository {

    private final StringRedisTemplate redis;

    @Override
    public void saveDropTitle(long dropId, String title) {
        put(RankingKeys.DROP_INFO, dropId, title);
    }

    @Override
    public void saveNickname(long userId, String nickname) {
        put(RankingKeys.USER_INFO, userId, nickname);
    }

    @Override
    public Map<Long, String> findDropTitles(Collection<Long> dropIds) {
        return multiGet(RankingKeys.DROP_INFO, dropIds);
    }

    @Override
    public Map<Long, String> findNicknames(Collection<Long> userIds) {
        return multiGet(RankingKeys.USER_INFO, userIds);
    }

    private void put(String key, long id, String value) {
        call(() -> {
            redis.opsForHash().put(key, String.valueOf(id), value);
            return null;
        });
    }

    /** HMGET. 빈 입력은 Redis 호출 없이 빈 맵, 중복 id 는 한 번만, 값이 없는 id 는 제외 */
    private Map<Long, String> multiGet(String key, Collection<Long> ids) {
        List<Long> distinct = ids.stream().distinct().toList();
        if (distinct.isEmpty()) {
            return Map.of();
        }
        List<Object> fields = distinct.stream().<Object>map(String::valueOf).toList();
        List<Object> values = call(() -> redis.opsForHash().multiGet(key, fields));
        Map<Long, String> result = new HashMap<>();
        for (int i = 0; i < distinct.size(); i++) {
            Object value = values.get(i);
            if (value != null) {
                result.put(distinct.get(i), (String) value);
            }
        }
        return result;
    }

    /** Redis 예외(DataAccessException 계열)를 포트 예외로 */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new RankingStoreUnavailableException("Redis 표시 정보 저장소 호출 실패", e);
        }
    }
}
