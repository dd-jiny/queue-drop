package com.queuedrop.queue.application.exception;

import com.queuedrop.common.error.ErrorCode;

public enum QueueErrorCode implements ErrorCode {
    DROP_NOT_FOUND("QUEUE_DROP_NOT_FOUND", 404, "대기열 정보가 없는 드로우입니다."),   // meta 없음 (drop.created 미수신 포함)
    NOT_OPEN("QUEUE_NOT_OPEN", 409, "아직 오픈 전입니다."),
    CLOSED("QUEUE_CLOSED", 409, "응모가 마감되었습니다."),
    SOLD_OUT("QUEUE_SOLD_OUT", 409, "응모 정원이 모두 찼습니다.");

    private final String code;
    private final int status;
    private final String message;

    QueueErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
