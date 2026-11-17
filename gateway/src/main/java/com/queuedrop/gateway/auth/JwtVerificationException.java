package com.queuedrop.gateway.auth;

public class JwtVerificationException extends RuntimeException {

    private final GatewayErrorCode errorCode;

    public JwtVerificationException(GatewayErrorCode errorCode, Throwable cause) {
        super(errorCode.message(), cause);
        this.errorCode = errorCode;
    }

    public GatewayErrorCode errorCode() {
        return errorCode;
    }
}
