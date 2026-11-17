package com.queuedrop.coupon.presentation.api.response;

import com.queuedrop.coupon.application.dto.CouponEventResult;
import java.time.Instant;

public record CouponEventResponse(Long id, String name, int totalQuantity, long remaining, Instant issueStartAt) {

    public static CouponEventResponse from(CouponEventResult result) {
        return new CouponEventResponse(result.id(), result.name(), result.totalQuantity(), result.remaining(),
                result.issueStartAt());
    }
}
