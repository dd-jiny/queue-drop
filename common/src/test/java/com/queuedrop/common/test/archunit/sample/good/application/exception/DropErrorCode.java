package com.queuedrop.common.test.archunit.sample.good.application.exception;

import com.queuedrop.common.error.ErrorCode;

public enum DropErrorCode implements ErrorCode {
    DROP_NOT_FOUND;

    @Override public String code() { return name(); }
    @Override public int status() { return 404; }
    @Override public String message() { return "not found"; }
}
