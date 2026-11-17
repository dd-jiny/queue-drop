package com.queuedrop.coupon.application.dto;

import java.time.Instant;

public record CouponEventResult(Long id, String name, int totalQuantity, long remaining, Instant issueStartAt) {}
