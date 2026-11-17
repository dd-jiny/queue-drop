package com.queuedrop.coupon.domain.repository;

import com.queuedrop.coupon.domain.model.IssuedCoupon;
import java.util.List;

/** 발급 쿠폰 영속성 포트. */
public interface IssuedCouponRepository {

    /** UK(coupon_event_id, user_id) 충돌 시 저장하지 않고 false */
    boolean saveIfAbsent(IssuedCoupon coupon);

    List<IssuedCoupon> findByUserId(long userId);
}
