package com.queuedrop.queue.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Redis 만 쓰는 컨슈머 실패 정책 (architecture.md 4장, D4): 1초 간격 3회 재시도 후 로그를 남기고 건너뛴다. DLT 없음.
 * Spring Boot 가 이 CommonErrorHandler 빈을 리스너 컨테이너 팩토리에 적용한다.
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 건너뛴다.
 */
@Configuration
public class KafkaConsumerConfig {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }
}
