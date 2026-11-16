package com.queuedrop.common.error;

import java.time.Instant;
import java.util.List;

/** 모든 서비스·게이트웨이 공통 에러 응답 바디. */
public record ErrorResponse(String code, String message, List<FieldError> fieldErrors, Instant timestamp) {

    public record FieldError(String field, String reason) {}

    public static ErrorResponse of(ErrorCode errorCode) {
        return new ErrorResponse(errorCode.code(), errorCode.message(), List.of(), Instant.now());
    }

    public static ErrorResponse of(ErrorCode errorCode, List<FieldError> fieldErrors) {
        return new ErrorResponse(errorCode.code(), errorCode.message(), fieldErrors, Instant.now());
    }
}
