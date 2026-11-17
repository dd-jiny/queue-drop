package com.queuedrop.coupon.application.dto;

import java.time.Instant;

public record CouponIssueResult(Long couponEventId, Instant issuedAt) {}
