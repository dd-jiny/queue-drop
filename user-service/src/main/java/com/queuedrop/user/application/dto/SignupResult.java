package com.queuedrop.user.application.dto;

import com.queuedrop.user.domain.model.User;

public record SignupResult(Long userId, String email, String nickname) {
    public static SignupResult from(User user) {
        return new SignupResult(user.getId(), user.getEmail().value(), user.getNickname());
    }
}
