package com.queuedrop.product.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

@Configuration
class ClockConfig {

    @Bean
    Clock clock() {
        // PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘다 — 응답·이벤트·DB 시각이 같아진다
        return Clock.tick(Clock.systemUTC(), Duration.ofNanos(1_000));
    }
}
