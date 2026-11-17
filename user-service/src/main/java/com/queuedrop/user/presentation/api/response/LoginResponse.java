package com.queuedrop.user.presentation.api.response;

import com.queuedrop.user.application.dto.LoginResult;

public record LoginResponse(String accessToken, String tokenType, long expiresIn) {

    public static LoginResponse from(LoginResult r) {
        return new LoginResponse(r.accessToken(), r.tokenType(), r.expiresInSeconds());
    }
}
