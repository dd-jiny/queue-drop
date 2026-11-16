package com.queuedrop.user.application.dto;

public record LoginResult(String accessToken, String tokenType, long expiresInSeconds) {}
