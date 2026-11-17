package com.queuedrop.common.event;

import java.time.Instant;

/** coupon-service → coupon(저장). key = "{couponEventId}:{userId}" */
public record CouponIssued(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}
