package com.queuedrop.user.application.dto;

public record CreateAdminCommand(String email, String rawPassword, String nickname) {}
