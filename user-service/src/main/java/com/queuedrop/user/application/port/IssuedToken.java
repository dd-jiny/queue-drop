package com.queuedrop.user.application.port;

public record IssuedToken(String value, long expiresInSeconds) {}
