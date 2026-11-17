package com.queuedrop.entry.infrastructure.redis;

import com.queuedrop.entry.application.port.CompensationResult;
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
    public GateResult tryApply(long dropId, long userId, String eventId, Instant now) {
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
    public CompensationResult compensate(long dropId, long userId, String eventId) {
        // TODO(#10b): entry-compensate.lua 3결과로 교체. 지금은 기존 Lua(1 = 되돌림, 0 = 응모자 아님)를 매핑만 한다
        Long result = redis.execute(entryCompensateScript,
                List.of(EntryRedisKeys.remaining(dropId), EntryRedisKeys.applicants(dropId)),
                String.valueOf(userId));
        return Long.valueOf(1L).equals(result) ? CompensationResult.COMPENSATED : CompensationResult.NOT_OURS;
    }

    @Override
    public boolean claimForSave(long dropId, long userId, String eventId) {
        // TODO(#10b): entry-claim.lua 로 교체. 지금은 기존 저장 전 확인(isApplicant)과 같다
        return isApplicant(dropId, userId);
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
