package com.queuedrop.queue.application.config;

/** application 계층 설정 값 객체. 바인딩·검증은 infrastructure/config 에서 한다 */
public record AdmissionProperties(long intervalMs, int batchSize, long tokenTtlSeconds) {}
