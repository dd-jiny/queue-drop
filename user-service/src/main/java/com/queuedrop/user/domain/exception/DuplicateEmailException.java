package com.queuedrop.user.domain.exception;

/** 저장 시 이메일 유니크 위반. infrastructure 가 DB 예외를 이 예외로 번역한다. */
public class DuplicateEmailException extends RuntimeException {
    public DuplicateEmailException(String email) { super("duplicate email: " + email); }
}
