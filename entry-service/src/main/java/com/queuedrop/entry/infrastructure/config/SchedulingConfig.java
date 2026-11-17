package com.queuedrop.entry.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** DrawScheduler 활성화. Clock 빈은 ClockConfig 를 그대로 쓴다 (중복 빈 금지) */
@Configuration
@EnableScheduling
public class SchedulingConfig {}
