package com.queuedrop.common.error;

/** application 계층이 던지는 유일한 비즈니스 예외 타입. presentation 이 HTTP 로 매핑한다. */
public class ApplicationException extends RuntimeException {
    private final ErrorCode errorCode;

    public ApplicationException(ErrorCode errorCode) {
        super(errorCode.message());
        this.errorCode = errorCode;
    }

    public ApplicationException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.message(), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() { return errorCode; }
}
