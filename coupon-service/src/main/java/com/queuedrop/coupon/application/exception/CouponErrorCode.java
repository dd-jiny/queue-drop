package com.queuedrop.coupon.application.exception;

import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.coupon.domain.model.IssueOutcome;

/** 응답 code 는 {SERVICE}_{REASON}, 상수명은 사유만. 던질 때는 new ApplicationException(CouponErrorCode.X) */
public enum CouponErrorCode implements ErrorCode {
    EVENT_NOT_FOUND("COUPON_EVENT_NOT_FOUND", 404, "쿠폰 이벤트를 찾을 수 없습니다."),
    NOT_STARTED("COUPON_NOT_STARTED", 409, "아직 발급 시작 전입니다."),
    DUPLICATE("COUPON_DUPLICATE", 409, "이미 발급받은 쿠폰입니다."),
    SOLD_OUT("COUPON_SOLD_OUT", 409, "쿠폰이 모두 소진되었습니다."),
    TEMPORARILY_FAILED("COUPON_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요."),
    INVALID_EVENT("COUPON_INVALID_EVENT", 400, "쿠폰 이벤트 정보가 올바르지 않습니다.");

    private final String code;
    private final int status;
    private final String message;

    CouponErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }

    /** domain 판정 결과 → 에러 코드. 변환은 여기 한 곳에서만 한다. */
    public static CouponErrorCode from(IssueOutcome outcome) {
        return switch (outcome) {
            case NOT_FOUND -> EVENT_NOT_FOUND;      // Redis meta 없음 = 미등록 또는 초기화 실패
            case NOT_STARTED -> NOT_STARTED;
            case DUPLICATE -> DUPLICATE;
            case SOLD_OUT -> SOLD_OUT;
            case OK -> throw new IllegalArgumentException("OK 는 에러가 아닙니다");
        };
    }
}
