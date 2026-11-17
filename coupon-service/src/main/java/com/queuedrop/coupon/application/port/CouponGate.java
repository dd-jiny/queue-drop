package com.queuedrop.coupon.application.port;

import java.time.Instant;

/**
 * Redis 기반 선착순 발급 게이트. tryIssue·compensate·claimForSave 는 Redis 장애(명령 타임아웃 포함) 시
 * GateUnavailableException 을 던진다 (4단계 EntryGate 와 같은 계약, E-41).
 * couponEventId = 쿠폰 이벤트 ID, requestId = 발급 요청 ID(= coupon.issued 메시지의 eventId).
 */
public interface CouponGate {

    /** SET NX, 멱등 */
    void initialize(long couponEventId, int totalQuantity, Instant issueStartAt);

    /** OK 면 requests[userId] = requestId 를 함께 기록한다. requestId 는 호출자가 판정 전에 만든다 (E-40) */
    GateResult tryIssue(long couponEventId, long userId, String requestId, Instant now);

    /** @return ALREADY_PERSISTED(저장 확정됨, 되돌리지 않음) / NOT_OURS(이 요청의 발급 아님) / COMPENSATED */
    CompensationResult compensate(long couponEventId, long userId, String requestId);

    /** 저장 컨슈머가 DB 저장 전에 호출. false 면 유령 이벤트 — 저장하지 않는다. 재전달에도 true (멱등) */
    boolean claimForSave(long couponEventId, long userId, String requestId);

    long remaining(long couponEventId);
}
