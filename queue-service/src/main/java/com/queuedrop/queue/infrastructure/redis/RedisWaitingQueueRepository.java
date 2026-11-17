package com.queuedrop.queue.infrastructure.redis;

import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import io.micrometer.core.instrument.MeterRegistry;
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
    /** 입장 토큰 발급 수 (8단계 3.2) */
    private static final String ADMITTED_METRIC = "queue.admitted";

    private final StringRedisTemplate redis;
    private final RedisScript<Long> enqueueScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> admitScript;
    private final RedisScript<Long> saveMetaScript;
    private final MeterRegistry meterRegistry;

    /** HSET + PEXPIREAT + ZADD active-drops 를 save_meta.lua 하나로 (중간 실패로 meta 와 인덱스가 어긋나지 않게) */
    @Override
    public void saveMeta(QueueMeta meta) {
        redis.execute(saveMetaScript,
                List.of(QueueKeys.meta(meta.dropId()), QueueKeys.ACTIVE_DROPS),
                String.valueOf(meta.dropId()),
                String.valueOf(meta.openAt().toEpochMilli()),
                String.valueOf(meta.closeAt().toEpochMilli()),
                String.valueOf(meta.closeAt().plus(RETENTION_AFTER_CLOSE).toEpochMilli()));
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
        List<Long> admitted = ids == null ? List.of()
                : ids.stream().map(id -> Long.valueOf(String.valueOf(id))).toList();
        if (!admitted.isEmpty()) {
            meterRegistry.counter(ADMITTED_METRIC).increment(admitted.size());   // 요청 n 이 아니라 실제 입장 인원
        }
        return admitted;
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
