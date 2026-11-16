package com.queuedrop.user.domain.exception;

public class InvalidNicknameException extends RuntimeException {
    public InvalidNicknameException(String value) { super("invalid nickname: " + value); }
}
