package com.queuedrop.entry.infrastructure.redis;

import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/** EntryGate 의 Redis Lua 구현. 판정·보상·저장 확정·초기화는 각각 스크립트 하나로 원자 실행한다 */
@Component
@RequiredArgsConstructor
public class RedisEntryGate implements EntryGate {

    private static final String COMPENSATED_METRIC = "entry.apply.compensated";

    private final StringRedisTemplate redis;
    private final RedisScript<Long> entryInitializeScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> entryApplyScript;
    private final RedisScript<Long> entryCompensateScript;
    private final RedisScript<Long> entryClaimScript;
    /** entry.apply.compensated{result} 기록용 (8단계 3.2, T8-13) */
    private final MeterRegistry meterRegistry;

    @Override
    public GateResult tryApply(long dropId, long userId, String eventId, Instant now) {
        List<?> result = call(() -> redis.execute(entryApplyScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId),
                        EntryRedisKeys.applicants(dropId), EntryRedisKeys.seq(dropId),
                        EntryRedisKeys.queueToken(dropId, userId), EntryRedisKeys.requests(dropId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()), eventId));

        ApplyOutcome outcome = ApplyOutcome.valueOf((String) result.get(0));
        if (outcome != ApplyOutcome.OK) {
            return GateResult.rejected(outcome);
        }
        return GateResult.ok(Math.toIntExact((Long) result.get(1)));
    }

    @Override
    public CompensationResult compensate(long dropId, long userId, String eventId) {
        Long result = call(() -> redis.execute(entryCompensateScript,
                List.of(EntryRedisKeys.remaining(dropId), EntryRedisKeys.applicants(dropId),
                        EntryRedisKeys.requests(dropId), EntryRedisKeys.persisted(dropId)),
                String.valueOf(userId), eventId));
        CompensationResult compensation = switch (result.intValue()) {
            case 1 -> CompensationResult.COMPENSATED;
            case 0 -> CompensationResult.NOT_OURS;
            case 2 -> CompensationResult.ALREADY_PERSISTED;
            default -> throw new IllegalStateException("알 수 없는 보상 결과: " + result);
        };
        // Lua 호출 자체가 실패하면 위에서 예외로 빠져 세지 않는다 (요청은 outcome=TEMPORARILY_FAILED 로 잡힌다)
        meterRegistry.counter(COMPENSATED_METRIC, "result", compensation.name()).increment();
        return compensation;
    }

    @Override
    public boolean claimForSave(long dropId, long userId, String eventId) {
        Long result = call(() -> redis.execute(entryClaimScript,
                List.of(EntryRedisKeys.applicants(dropId), EntryRedisKeys.requests(dropId),
                        EntryRedisKeys.persisted(dropId)),
                String.valueOf(userId), eventId));
        return result == 1L;
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

    @Override
    public void close(long dropId) {
        // meta 가 없으면 state 만 있는 meta 가 생기지만 응모는 closeAt 검사로 차단되고 추첨 후 expireAfterDraw 가 정리한다
        call(() -> {
            redis.opsForHash().put(EntryRedisKeys.meta(dropId), "state", "CLOSED");
            return null;
        });
    }

    @Override
    public void expireAfterDraw(long dropId, Duration ttl) {
        long seconds = ttl.toSeconds();
        call(() -> redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String key : EntryRedisKeys.all(dropId)) {
                connection.keyCommands().expire(key.getBytes(StandardCharsets.UTF_8), seconds);
            }
            return null;
        }));
    }

    /** Redis 예외(연결 실패·명령 타임아웃 = QueryTimeoutException 등 DataAccessException 계열)를 포트 예외로 (v1.5, E-41) */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new GateUnavailableException("Redis 응모 게이트 호출 실패", e);
        }
    }
}
