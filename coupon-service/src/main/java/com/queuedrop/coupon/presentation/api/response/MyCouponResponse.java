package com.queuedrop.coupon.presentation.api.response;

import com.queuedrop.coupon.application.dto.MyCouponResult;
import java.time.Instant;

public record MyCouponResponse(Long couponEventId, String eventName, Instant issuedAt) {

    public static MyCouponResponse from(MyCouponResult result) {
        return new MyCouponResponse(result.couponEventId(), result.eventName(), result.issuedAt());
    }
}
