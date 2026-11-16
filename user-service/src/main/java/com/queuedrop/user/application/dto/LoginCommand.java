package com.queuedrop.user.application.dto;

public record LoginCommand(String email, String rawPassword) {}
