package com.queuedrop.common.test.archunit.sample.good.domain.exception;

/** 순수 Java 도메인 예외 */
public class DomainException extends RuntimeException {
    public DomainException(String message) { super(message); }
}
