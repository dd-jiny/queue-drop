package com.queuedrop.user.application.service.fake;

import com.queuedrop.user.application.port.PasswordEncoder;

/** 결정적인 가짜 해시: "hashed:" + 원문. */
public class FakePasswordEncoder implements PasswordEncoder {

    private static final String PREFIX = "hashed:";

    @Override
    public String encode(String rawPassword) {
        return PREFIX + rawPassword;
    }

    @Override
    public boolean matches(String rawPassword, String passwordHash) {
        return passwordHash != null && passwordHash.equals(PREFIX + rawPassword);
    }
}
