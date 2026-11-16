package com.queuedrop.product.domain.exception;

/** 도메인 불변식 위반. application 이 받아 ProductErrorCode 로 변환한다 (메시지는 로그에만 남는다). */
public class DomainException extends RuntimeException {
    public DomainException(String message) {
        super(message);
    }
}
