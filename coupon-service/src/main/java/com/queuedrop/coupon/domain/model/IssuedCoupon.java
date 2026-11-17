package com.queuedrop.coupon.domain.model;

import java.time.Instant;
import java.util.Objects;
import lombok.Getter;

/** 발급된 쿠폰 한 건. 1인 1매·수량 판정은 Redis Lua 가 끝낸 뒤 만든다. */
@Getter
public final class IssuedCoupon {

    private final Long id;             // 저장 전 null
    private final Long couponEventId;
    private final Long userId;
    private final Instant issuedAt;

    private IssuedCoupon(Long id, Long couponEventId, Long userId, Instant issuedAt) {
        this.id = id;
        this.couponEventId = couponEventId;
        this.userId = userId;
        this.issuedAt = issuedAt;
    }

    public static IssuedCoupon issue(Long couponEventId, Long userId, Instant issuedAt) {
        Objects.requireNonNull(couponEventId, "couponEventId");
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(issuedAt, "issuedAt");
        return new IssuedCoupon(null, couponEventId, userId, issuedAt);
    }

    /** 저장소 복원용. 저장 당시 검증을 통과한 값이므로 다시 검증하지 않는다. */
    public static IssuedCoupon restore(Long id, Long couponEventId, Long userId, Instant issuedAt) {
        return new IssuedCoupon(id, couponEventId, userId, issuedAt);
    }
}
