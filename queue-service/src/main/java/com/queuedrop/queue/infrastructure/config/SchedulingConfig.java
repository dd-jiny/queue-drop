package com.queuedrop.queue.infrastructure.config;

import com.queuedrop.queue.application.config.AdmissionProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** queue.admission.* 바인딩·검증. AdmissionProperties 는 application 의 순수 값 객체 */
@Configuration
@EnableScheduling
public class SchedulingConfig {

    static final int MAX_BATCH_SIZE = 1_000;   // admit.lua 반복 상한 (Redis 블로킹 시간)

    @Bean
    AdmissionProperties admissionProperties(
            @Value("${queue.admission.interval-ms}") long intervalMs,
            @Value("${queue.admission.batch-size}") int batchSize,
            @Value("${queue.admission.token-ttl-seconds}") long tokenTtlSeconds) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalStateException("queue.admission.batch-size 는 1~" + MAX_BATCH_SIZE + " 이어야 합니다: " + batchSize);
        }
        if (intervalMs < 1 || tokenTtlSeconds < 1) {
            throw new IllegalStateException("queue.admission.interval-ms·token-ttl-seconds 는 1 이상이어야 합니다");
        }
        return new AdmissionProperties(intervalMs, batchSize, tokenTtlSeconds);
    }
}
