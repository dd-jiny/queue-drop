package com.queuedrop.coupon.application.dto;

import java.time.Instant;

public record MyCouponResult(Long couponEventId, String eventName, Instant issuedAt) {}
