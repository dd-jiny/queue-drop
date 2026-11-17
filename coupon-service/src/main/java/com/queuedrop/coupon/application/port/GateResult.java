package com.queuedrop.coupon.application.port;

import com.queuedrop.coupon.domain.model.IssueOutcome;

/** 발급 판정 결과 (포트 반환 타입, application 안에서만 쓰인다) */
public record GateResult(IssueOutcome outcome) {}
