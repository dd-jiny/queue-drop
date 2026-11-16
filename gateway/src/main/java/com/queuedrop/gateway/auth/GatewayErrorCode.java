package com.queuedrop.gateway.auth;

import com.queuedrop.common.error.ErrorCode;

/** 인증 실패 코드. 접두어 AUTH_ 는 인증 도메인 이름 (명명 규칙 {SERVICE}_{REASON} 의 예외, phase-1 v1.2) */
public enum GatewayErrorCode implements ErrorCode {
    TOKEN_MISSING("AUTH_TOKEN_MISSING", 401, "인증 토큰이 필요합니다."),
    INVALID_TOKEN("AUTH_INVALID_TOKEN", 401, "유효하지 않은 인증 토큰입니다."),
    EXPIRED_TOKEN("AUTH_EXPIRED_TOKEN", 401, "인증 토큰이 만료되었습니다.");

    private final String code;
    private final int status;
    private final String message;

    GatewayErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
