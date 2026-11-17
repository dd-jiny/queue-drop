package com.queuedrop.coupon.presentation.api.response;

import com.queuedrop.coupon.application.dto.CouponIssueResult;
import java.time.Instant;

public record CouponIssueResponse(Long couponEventId, Instant issuedAt) {

    public static CouponIssueResponse from(CouponIssueResult result) {
        return new CouponIssueResponse(result.couponEventId(), result.issuedAt());
    }
}
