package com.queuedrop.user.presentation.bootstrap;

import com.queuedrop.user.application.dto.CreateAdminCommand;
import com.queuedrop.user.application.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** 기동이라는 '입력'을 받아 application 을 호출하는 입력 어댑터 → presentation (Scheduler·Listener 와 같은 위치) */
@Slf4j
@Component
@RequiredArgsConstructor
@EnableConfigurationProperties(AdminAccountProperties.class)
class AdminAccountInitializer implements ApplicationRunner {

    private static final int PASSWORD_MIN = 8;
    private static final int PASSWORD_MAX_BYTES = 72;   // BCrypt 제한

    private final AdminAccountProperties properties;
    private final UserService userService;

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.enabled()) {
            log.info("ADMIN_EMAIL not set — skip admin bootstrap");
            return;
        }
        String password = properties.password();
        if (password == null || password.length() < PASSWORD_MIN
                || password.getBytes(StandardCharsets.UTF_8).length > PASSWORD_MAX_BYTES) {
            // 기동 실패로 설정 오류를 빨리 드러낸다
            throw new IllegalStateException("ADMIN_PASSWORD must be at least " + PASSWORD_MIN
                    + " characters and at most " + PASSWORD_MAX_BYTES + " UTF-8 bytes");
        }
        boolean created = userService.createAdminIfAbsent(
                new CreateAdminCommand(properties.email(), properties.password(), properties.nickname()));
        log.info(created ? "admin account created: {}" : "admin account already exists: {}", properties.email());
    }
}
