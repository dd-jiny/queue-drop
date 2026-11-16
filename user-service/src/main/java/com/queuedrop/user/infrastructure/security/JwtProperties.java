package com.queuedrop.user.infrastructure.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "queuedrop.jwt")
public record JwtProperties(String secret, String issuer, Duration accessTokenTtl) {}
