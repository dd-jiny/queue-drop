package com.queuedrop.coupon.application.dto;

import java.time.Instant;

public record CouponEventRegisterCommand(Requester requester, String name, int totalQuantity, Instant issueStartAt) {}
