package com.queuedrop.coupon.domain.model;

/** Redis Lua 발급 판정 결과. 판정 자체는 원자성을 위해 Lua 에 위임한다. */
public enum IssueOutcome {
    OK, NOT_STARTED, DUPLICATE, SOLD_OUT,
    NOT_FOUND        // Redis 에 이벤트 meta 가 없음 (미등록 또는 초기화 실패)
}
