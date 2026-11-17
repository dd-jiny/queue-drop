package com.queuedrop.ranking.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Redis 만 쓰는 컨슈머 실패 정책 (architecture.md 4장, queue KafkaConsumerConfig 와 같음): 1초 간격 3회 재시도 후
 * 로그를 남기고 건너뛴다. DLT 없음 (확정, 2026-11-16 — 랭킹은 응모의 진실의 원천이 아니고 재집계로 복구).
 * Spring Boot 가 이 CommonErrorHandler 빈 하나를 리스너 컨테이너 팩토리에 자동 적용한다.
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 recoverer 로 간다 (파티션을 막지 않음).
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        ConsumerRecordRecoverer recoverer = (record, ex) ->
                log.error("ranking 집계 실패 — 건너뜀 topic={} partition={} offset={} key={}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }
}
