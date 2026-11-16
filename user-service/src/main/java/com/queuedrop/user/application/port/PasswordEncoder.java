package com.queuedrop.user.application.port;

/** Spring Security 의 PasswordEncoder 를 직접 쓰지 않기 위한 포트 */
public interface PasswordEncoder {
    String encode(String rawPassword);
    boolean matches(String rawPassword, String passwordHash);
}
