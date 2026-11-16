package com.queuedrop.entry.application.exception;

import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.entry.domain.model.ApplyOutcome;

/** 응답 code 는 {SERVICE}_{REASON}, 상수명은 사유만. 상태 코드는 enum 이 갖는다. */
public enum EntryErrorCode implements ErrorCode {
    CLOSED("ENTRY_CLOSED", 409, "응모 기간이 아닙니다."),
    NO_TOKEN("ENTRY_NO_TOKEN", 409, "입장 토큰이 없거나 만료되었습니다."),
    DUPLICATE("ENTRY_DUPLICATE", 409, "이미 응모한 드로우입니다."),
    SOLD_OUT("ENTRY_SOLD_OUT", 409, "응모 정원이 마감되었습니다."),
    TEMPORARILY_FAILED("ENTRY_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요."),
    NOT_FOUND("ENTRY_NOT_FOUND", 404, "응모 내역이 없습니다.");      // 단건 조회용

    private final String code;
    private final int status;
    private final String message;

    EntryErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }

    /** domain 판정 결과 → application 에러 코드. 변환은 여기 한 곳에서만 한다. */
    public static EntryErrorCode from(ApplyOutcome outcome) {
        return switch (outcome) {
            case CLOSED -> CLOSED;
            case NO_TOKEN -> NO_TOKEN;
            case DUPLICATE -> DUPLICATE;
            case SOLD_OUT -> SOLD_OUT;
            case OK -> throw new IllegalArgumentException("OK 는 에러가 아닙니다");
        };
    }
}
