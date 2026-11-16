package com.queuedrop.user.application.exception;

import com.queuedrop.common.error.ErrorCode;

public enum UserErrorCode implements ErrorCode {
    DUPLICATE_EMAIL("USER_DUPLICATE_EMAIL", 409, "이미 가입된 이메일입니다."),
    INVALID_EMAIL("USER_INVALID_EMAIL", 400, "이메일 형식이 올바르지 않습니다."),
    INVALID_NICKNAME("USER_INVALID_NICKNAME", 400, "닉네임은 2~20자여야 합니다."),
    INVALID_CREDENTIALS("USER_INVALID_CREDENTIALS", 401, "이메일 또는 비밀번호가 올바르지 않습니다.");

    private final String code;
    private final int status;
    private final String message;

    UserErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
