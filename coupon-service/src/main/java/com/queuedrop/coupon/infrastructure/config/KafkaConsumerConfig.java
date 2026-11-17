package com.queuedrop.coupon.infrastructure.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * DB 에 저장하는 컨슈머 실패 정책 (entry KafkaConsumerConfig 와 같은 형태): 1초 간격 3회 재시도 후 {topic}.DLT.
 * DLT 재발행은 JSON·bytes 둘 다 전용 producer 로 max.block.ms 5초를 준다 (E-50) —
 * 공통 producer 의 1초를 물려받으면 첫 DLT 전송이 메타데이터 대기로 실패해 재처리 루프에 들어갈 수 있다.
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 DLT 로 간다.
 */
@Configuration
public class KafkaConsumerConfig implements DisposableBean {

    static final long DLT_MAX_BLOCK_MS = 5_000L;
    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;
    static final String DLT_SUFFIX = ".DLT";

    /** 둘 다 빈이 아니다 (KafkaTemplate·ProducerFactory 빈을 더 두면 Boot 자동 구성 빈이 빠진다) → destroy() 에서 닫는다 */
    private DefaultKafkaProducerFactory<String, Object> dltJsonFactory;
    private DefaultKafkaProducerFactory<String, byte[]> dltBytesFactory;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<String, Object> producerFactory) {
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, DLT_MAX_BLOCK_MS);
        dltJsonFactory = new DefaultKafkaProducerFactory<>(config);              // 기존 직렬화 설정 그대로
        dltBytesFactory = new DefaultKafkaProducerFactory<>(config, new StringSerializer(), new ByteArraySerializer());

        // 값 타입 순서 매칭: 역직렬화 실패 레코드(byte[])는 원본 바이트 그대로, 나머지는 JSON
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, new KafkaTemplate<>(dltBytesFactory));
        templates.put(Object.class, new KafkaTemplate<>(dltJsonFactory));

        // Spring Kafka 4 기본 목적지는 {topic}-dlt — coupon.issued.DLT, 원본과 같은 파티션으로 지정
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }

    @Override
    public void destroy() {
        if (dltJsonFactory != null) {
            dltJsonFactory.destroy();
        }
        if (dltBytesFactory != null) {
            dltBytesFactory.destroy();
        }
    }
}
