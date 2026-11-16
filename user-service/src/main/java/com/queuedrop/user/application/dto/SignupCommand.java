package com.queuedrop.user.application.dto;

public record SignupCommand(String email, String rawPassword, String nickname) {}
