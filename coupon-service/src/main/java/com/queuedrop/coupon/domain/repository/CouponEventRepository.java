package com.queuedrop.coupon.domain.repository;

import com.queuedrop.coupon.domain.model.CouponEvent;
import java.util.Optional;

/** 쿠폰 이벤트 영속성 포트. */
public interface CouponEventRepository {

    CouponEvent save(CouponEvent event);

    Optional<CouponEvent> findById(long id);
}
