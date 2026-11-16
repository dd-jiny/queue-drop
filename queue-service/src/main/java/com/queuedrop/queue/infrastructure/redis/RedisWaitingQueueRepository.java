package com.queuedrop.queue.infrastructure.redis;

import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class RedisWaitingQueueRepository implements WaitingQueueRepository {

    /** closeAt 이후 queue 키(meta·waiting·seq)를 남겨 두는 시간 (기본안 1일) */
    static final Duration RETENTION_AFTER_CLOSE = Duration.ofDays(1);

    private static final String OPEN_AT = "openAt";
    private static final String CLOSE_AT = "closeAt";

    private final StringRedisTemplate redis;
    private final RedisScript<Long> enqueueScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> admitScript;

    @Override
    public void saveMeta(QueueMeta meta) {
        String key = QueueKeys.meta(meta.dropId());
        redis.opsForHash().putAll(key, Map.of(
                OPEN_AT, String.valueOf(meta.openAt().toEpochMilli()),
                CLOSE_AT, String.valueOf(meta.closeAt().toEpochMilli())));
        redis.expireAt(key, meta.closeAt().plus(RETENTION_AFTER_CLOSE));   // 이미 지났으면 즉시 삭제
        redis.opsForZSet().add(QueueKeys.ACTIVE_DROPS, String.valueOf(meta.dropId()),
                meta.closeAt().toEpochMilli());
    }

    @Override
    public Optional<QueueMeta> findMeta(Long dropId) {
        Map<Object, Object> h = redis.opsForHash().entries(QueueKeys.meta(dropId));
        if (h.isEmpty()) return Optional.empty();
        return Optional.of(new QueueMeta(dropId,
                Instant.ofEpochMilli(Long.parseLong((String) h.get(OPEN_AT))),
                Instant.ofEpochMilli(Long.parseLong((String) h.get(CLOSE_AT)))));
    }

    @Override
    public EnqueueResult enqueue(Long dropId, Long userId) {
        Long r = redis.execute(enqueueScript,
                List.of(QueueKeys.waiting(dropId), QueueKeys.seq(dropId),
                        QueueKeys.token(dropId, userId), QueueKeys.meta(dropId)),
                String.valueOf(userId), String.valueOf(RETENTION_AFTER_CLOSE.toMillis()));
        if (r == null) throw new IllegalStateException("enqueue.lua 가 값을 반환하지 않았습니다: dropId=" + dropId);
        return r == -1 ? EnqueueResult.alreadyAdmitted() : EnqueueResult.waiting(r);
    }

    @Override
    public boolean hasToken(Long dropId, Long userId) {
        return Boolean.TRUE.equals(redis.hasKey(QueueKeys.token(dropId, userId)));
    }

    @Override
    public OptionalLong rank(Long dropId, Long userId) {
        Long r = redis.opsForZSet().rank(QueueKeys.waiting(dropId), String.valueOf(userId));
        return r == null ? OptionalLong.empty() : OptionalLong.of(r);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Long> admit(Long dropId, int n, Duration tokenTtl, Instant now) {
        List<Object> ids = redis.execute(admitScript,
                List.of(QueueKeys.waiting(dropId), QueueKeys.entryRemaining(dropId)),
                String.valueOf(n), String.valueOf(tokenTtl.toSeconds()),
                QueueKeys.tokenPrefix(dropId), String.valueOf(now.toEpochMilli()));
        return ids == null ? List.of() : ids.stream().map(id -> Long.valueOf(String.valueOf(id))).toList();
    }

    @Override
    public List<Long> findActiveDropIds(Instant now) {
        Set<String> ids = redis.opsForZSet().rangeByScore(QueueKeys.ACTIVE_DROPS,
                now.toEpochMilli() + 1, Double.POSITIVE_INFINITY);   // closeAt > now
        return ids == null ? List.of() : ids.stream().map(Long::valueOf).toList();
    }

    @Override
    public void removeExpiredDrops(Instant now) {
        redis.opsForZSet().removeRangeByScore(QueueKeys.ACTIVE_DROPS, Double.NEGATIVE_INFINITY, now.toEpochMilli());
    }
}
