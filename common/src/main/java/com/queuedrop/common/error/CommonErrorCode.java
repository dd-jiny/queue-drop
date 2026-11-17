package com.queuedrop.common.error;

public enum CommonErrorCode implements ErrorCode {
    INVALID_INPUT("COMMON_INVALID_INPUT", 400, "요청 값이 올바르지 않습니다."),
    UNAUTHORIZED("COMMON_UNAUTHORIZED", 401, "인증이 필요합니다."),
    FORBIDDEN("COMMON_FORBIDDEN", 403, "권한이 없습니다."),
    NOT_FOUND("COMMON_NOT_FOUND", 404, "대상을 찾을 수 없습니다."),
    METHOD_NOT_ALLOWED("COMMON_METHOD_NOT_ALLOWED", 405, "지원하지 않는 요청 메서드입니다."),
    UNSUPPORTED_MEDIA_TYPE("COMMON_UNSUPPORTED_MEDIA_TYPE", 415, "지원하지 않는 콘텐츠 형식입니다."),
    MISSING_USER_HEADER("COMMON_MISSING_USER_HEADER", 400, "사용자 식별 헤더가 없습니다."),
    INTERNAL_ERROR("COMMON_INTERNAL_ERROR", 500, "일시적인 오류가 발생했습니다.");

    private final String code;
    private final int status;
    private final String message;

    CommonErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
