package com.queuedrop.common.test.archunit.sample.bad.r3.metrics.application.service;

import io.micrometer.core.instrument.MeterRegistry;

/** 8단계: 커스텀 메트릭은 presentation·infrastructure 에서만 기록한다 (phase-8 3.2) */
public class MeteredService {
    private MeterRegistry meterRegistry;
}
