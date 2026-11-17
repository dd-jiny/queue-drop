package com.queuedrop.ranking.domain.exception;

public class InvalidWeekKeyException extends DomainException {
    public InvalidWeekKeyException(String value) {
        super("유효하지 않은 주차: " + value);
    }
}
