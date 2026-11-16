package com.queuedrop.common.error;

/** application 예외의 공통 부모. 서비스별 예외(EntryApplyException 등)는 이를 상속한다. presentation 이 HTTP 로 매핑한다. */
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
