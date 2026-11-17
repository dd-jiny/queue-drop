package com.queuedrop.coupon.infrastructure.persistence;

import com.queuedrop.coupon.domain.model.IssuedCoupon;

/** 저장은 native INSERT 로 필드를 직접 넘기므로 toEntity 는 두지 않는다. */
final class IssuedCouponMapper {

    private IssuedCouponMapper() {
    }

    static IssuedCoupon toDomain(IssuedCouponJpaEntity e) {
        return IssuedCoupon.restore(e.getId(), e.getCouponEventId(), e.getUserId(), e.getIssuedAt());
    }
}
