package com.queuedrop.user.domain.exception;

public class InvalidEmailException extends RuntimeException {
    public InvalidEmailException(String value) { super("invalid email: " + value); }
}
