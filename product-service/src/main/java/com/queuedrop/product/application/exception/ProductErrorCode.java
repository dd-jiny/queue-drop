package com.queuedrop.product.application.exception;

import com.queuedrop.common.error.ErrorCode;

/** 권한 없음은 서비스 고유 사유가 아니므로 CommonErrorCode.FORBIDDEN 을 쓴다. */
public enum ProductErrorCode implements ErrorCode {
    PRODUCT_NOT_FOUND("PRODUCT_NOT_FOUND", 404, "상품을 찾을 수 없습니다."),
    DROP_NOT_FOUND("PRODUCT_DROP_NOT_FOUND", 404, "드로우를 찾을 수 없습니다."),
    INVALID_DROP("PRODUCT_INVALID_DROP", 400, "드로우 정보가 올바르지 않습니다."),
    INVALID_PRODUCT("PRODUCT_INVALID_PRODUCT", 400, "상품 정보가 올바르지 않습니다."),
    INVALID_STATUS_FILTER("PRODUCT_INVALID_STATUS_FILTER", 400, "알 수 없는 상태 값입니다.");

    private final String code;
    private final int status;
    private final String message;

    ProductErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
