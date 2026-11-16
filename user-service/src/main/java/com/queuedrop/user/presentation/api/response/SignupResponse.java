package com.queuedrop.user.presentation.api.response;

import com.queuedrop.user.application.dto.SignupResult;

public record SignupResponse(Long userId, String email, String nickname) {

    public static SignupResponse from(SignupResult r) {
        return new SignupResponse(r.userId(), r.email(), r.nickname());
    }
}
