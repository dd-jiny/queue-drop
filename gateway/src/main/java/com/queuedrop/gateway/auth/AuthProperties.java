package com.queuedrop.gateway.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "queuedrop")
public record AuthProperties(Jwt jwt, Auth auth) {

    public record Jwt(String secret, String issuer) {}

    /** publicPaths: "METHOD /path/{var}" 형식 */
    public record Auth(List<String> publicPaths) {}
}
