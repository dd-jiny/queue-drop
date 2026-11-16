package com.queuedrop.gateway.filter;

/** gateway 가 다운스트림 서비스에 전달하는 인증 사용자 헤더 */
public final class UserHeaders {
    public static final String USER_ID = "X-User-Id";
    public static final String USER_ROLE = "X-User-Role";

    private UserHeaders() {}
}
