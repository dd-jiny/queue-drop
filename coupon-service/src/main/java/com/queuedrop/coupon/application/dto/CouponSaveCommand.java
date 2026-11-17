package com.queuedrop.coupon.application.dto;

import java.time.Instant;

/** eventId = 요청 ID */
public record CouponSaveCommand(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}
