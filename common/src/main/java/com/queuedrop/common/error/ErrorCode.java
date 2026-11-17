package com.queuedrop.common.error;

/** 서비스별 에러 코드 enum 이 구현한다 (예: UserErrorCode). */
public interface ErrorCode {
    String code();       // 예: "USER_DUPLICATE_EMAIL"
    int status();        // HTTP 상태 코드
    String message();    // 사용자 노출 기본 메시지
}
