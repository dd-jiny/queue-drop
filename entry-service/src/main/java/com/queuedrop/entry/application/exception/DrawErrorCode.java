package com.queuedrop.entry.application.exception;

import com.queuedrop.common.error.ErrorCode;

/** 추첨 에러 코드. 던질 때는 new ApplicationException(DrawErrorCode.X). 권한 없음은 CommonErrorCode.FORBIDDEN */
public enum DrawErrorCode implements ErrorCode {
    DROP_NOT_FOUND("ENTRY_DRAW_DROP_NOT_FOUND", 404, "드로우를 찾을 수 없습니다."),
    NOT_COMPLETED("ENTRY_DRAW_NOT_COMPLETED", 409, "아직 추첨이 완료되지 않았습니다."),
    NOT_CLOSING("ENTRY_DRAW_NOT_CLOSING", 409, "마감 처리 중인 드로우만 수동 추첨할 수 있습니다."),  // D5-1
    ALREADY_DRAWN("ENTRY_DRAW_ALREADY_DRAWN", 409, "이미 추첨된 드로우입니다.");                   // D5-1

    private final String code;
    private final int status;
    private final String message;

    DrawErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
