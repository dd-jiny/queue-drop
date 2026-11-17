package com.queuedrop.coupon.infrastructure.redis;

import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.application.port.GateResult;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/** CouponGate 의 Redis Lua 구현. 판정·보상·저장 확정은 각각 스크립트 하나로 원자 실행한다 */
@Component
@RequiredArgsConstructor
public class RedisCouponGate implements CouponGate {

    private final StringRedisTemplate redis;
    // 빈 이름 = 필드 이름 (infrastructure/config/RedisScriptConfig)
    private final RedisScript<String> couponIssueScript;
    private final RedisScript<Long> couponCompensateScript;
    private final RedisScript<Long> couponClaimScript;

    @Override
    public void initialize(long couponEventId, int totalQuantity, Instant issueStartAt) {
        call(() -> {
            redis.opsForValue().setIfAbsent(CouponKeys.remaining(couponEventId), String.valueOf(totalQuantity));   // SET NX
            redis.opsForHash().putIfAbsent(CouponKeys.meta(couponEventId), "issueStartAt",
                    String.valueOf(issueStartAt.toEpochMilli()));                                                  // HSETNX
            return null;
        });
    }

    @Override
    public GateResult tryIssue(long couponEventId, long userId, String requestId, Instant now) {
        String result = call(() -> redis.execute(couponIssueScript,
                List.of(CouponKeys.meta(couponEventId), CouponKeys.remaining(couponEventId),
                        CouponKeys.issued(couponEventId), CouponKeys.requests(couponEventId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()), requestId));
        return new GateResult(IssueOutcome.valueOf(result));
    }

    @Override
    public CompensationResult compensate(long couponEventId, long userId, String requestId) {
        Long result = call(() -> redis.execute(couponCompensateScript,
                List.of(CouponKeys.remaining(couponEventId), CouponKeys.issued(couponEventId),
                        CouponKeys.requests(couponEventId), CouponKeys.persisted(couponEventId)),
                String.valueOf(userId), requestId));
        return switch (result.intValue()) {
            case 1 -> CompensationResult.COMPENSATED;
            case 0 -> CompensationResult.NOT_OURS;
            case 2 -> CompensationResult.ALREADY_PERSISTED;
            default -> throw new IllegalStateException("알 수 없는 보상 결과: " + result);
        };
    }

    @Override
    public boolean claimForSave(long couponEventId, long userId, String requestId) {
        Long result = call(() -> redis.execute(couponClaimScript,
                List.of(CouponKeys.issued(couponEventId), CouponKeys.requests(couponEventId),
                        CouponKeys.persisted(couponEventId)),
                String.valueOf(userId), requestId));
        return result == 1L;
    }

    @Override
    public long remaining(long couponEventId) {
        String value = call(() -> redis.opsForValue().get(CouponKeys.remaining(couponEventId)));
        return value == null ? 0L : Long.parseLong(value);
    }

    /** Redis 예외(연결 실패·명령 타임아웃 = QueryTimeoutException 등 DataAccessException 계열)를 포트 예외로 (E-41) */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new GateUnavailableException("Redis 쿠폰 게이트 호출 실패", e);
        }
    }
}
