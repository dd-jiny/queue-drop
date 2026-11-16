package com.queuedrop.user.presentation.bootstrap;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "queuedrop.admin")
public record AdminAccountProperties(String email, String password, String nickname) {

    public boolean enabled() {
        return email != null && !email.isBlank();
    }
}
