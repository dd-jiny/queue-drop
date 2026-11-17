package com.queuedrop.coupon.application.dto;

public record CouponReinitializeCommand(Requester requester, Long couponEventId) {}
