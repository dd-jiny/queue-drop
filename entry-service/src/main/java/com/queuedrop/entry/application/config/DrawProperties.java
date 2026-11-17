package com.queuedrop.entry.application.config;

import java.time.Duration;

/** entry.draw.* 설정 값 객체 (application 순수 record). 바인딩은 infrastructure/config 에서 한다 */
public record DrawProperties(Duration closingWarnThreshold, Duration keyTtlAfterDraw) {}
