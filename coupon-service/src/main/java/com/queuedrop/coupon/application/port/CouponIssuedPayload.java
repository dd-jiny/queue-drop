package com.queuedrop.coupon.application.port;

import java.time.Instant;

/** application 자체 DTO. common 의 CouponIssued 메시지로의 변환은 infrastructure 가 맡는다. eventId = 요청 ID */
public record CouponIssuedPayload(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}
