package com.queuedrop.ranking.application.exception;

import com.queuedrop.common.error.ErrorCode;

/** 응답 code 는 {SERVICE}_{REASON}, 상수명은 사유만. 던질 때는 new ApplicationException(RankingErrorCode.X) */
public enum RankingErrorCode implements ErrorCode {
    INVALID_SIZE("RANKING_INVALID_SIZE", 400, "size 는 1 이상 100 이하여야 합니다."),
    INVALID_WEEK("RANKING_INVALID_WEEK", 400, "주차 형식이 올바르지 않습니다. (예: 2026-W47)"),
    TEMPORARILY_FAILED("RANKING_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요.");

    private final String code;
    private final int status;
    private final String message;

    RankingErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
