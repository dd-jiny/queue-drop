package com.queuedrop.entry.infrastructure.redis;

import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/** EntryGate 의 Redis Lua 구현. 판정·보상·초기화는 각각 스크립트 하나로 원자 실행한다 */
@Component
@RequiredArgsConstructor
public class RedisEntryGate implements EntryGate {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> entryInitializeScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> entryApplyScript;
    private final RedisScript<Long> entryCompensateScript;

    @Override
    public GateResult tryApply(long dropId, long userId, Instant now) {
        List<?> result = redis.execute(entryApplyScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId),
                        EntryRedisKeys.applicants(dropId), EntryRedisKeys.seq(dropId),
                        EntryRedisKeys.queueToken(dropId, userId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()));

        ApplyOutcome outcome = ApplyOutcome.valueOf((String) result.get(0));
        if (outcome != ApplyOutcome.OK) {
            return GateResult.rejected(outcome);
        }
        return GateResult.ok(Math.toIntExact((Long) result.get(1)));
    }

    @Override
    public void compensate(long dropId, long userId) {
        redis.execute(entryCompensateScript,
                List.of(EntryRedisKeys.remaining(dropId), EntryRedisKeys.applicants(dropId)),
                String.valueOf(userId));
    }

    @Override
    public boolean isApplicant(long dropId, long userId) {
        return Boolean.TRUE.equals(
                redis.opsForSet().isMember(EntryRedisKeys.applicants(dropId), String.valueOf(userId)));
    }

    @Override
    public void initialize(long dropId, int capacity, Instant openAt, Instant closeAt) {
        redis.execute(entryInitializeScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId)),
                String.valueOf(capacity),
                String.valueOf(openAt.toEpochMilli()), String.valueOf(closeAt.toEpochMilli()));
    }
}
