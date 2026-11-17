package com.queuedrop.entry.infrastructure.config;

import java.util.LinkedHashMap;
import java.util.Map;
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
 * DB 에 저장하는 컨슈머 실패 정책 (architecture.md 4장, 6.1 ③): 1초 간격 3회 재시도 후 {topic}.DLT.
 * Spring Boot 가 이 CommonErrorHandler 빈을 기본 리스너 컨테이너 팩토리에 적용한다.
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 DLT 로 간다.
 * Spring Kafka 4 의 기본 DLT 이름은 {topic}-dlt 라서, Topics.*_DLT({topic}.DLT) 로 보내도록 목적지를 명시한다.
 */
@Configuration
public class KafkaConsumerConfig implements DisposableBean {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;
    static final String DLT_SUFFIX = ".DLT";

    /** 빈이 아니라서 Spring 이 닫지 않는다 → destroy() 에서 직접 닫는다 */
    private DefaultKafkaProducerFactory<String, byte[]> bytesProducerFactory;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate,
                                          ProducerFactory<String, Object> producerFactory) {
        // 값 타입 순서 매칭: 역직렬화 실패 레코드(byte[])는 원본 바이트 그대로, 나머지는 JSON 템플릿으로
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate(producerFactory));
        templates.put(Object.class, kafkaTemplate);

        // {topic}.DLT, 원본과 같은 파티션 번호 (DLT 토픽은 원본과 같은 파티션 수로 만든다)
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }

    /**
     * 기존 producer 설정을 복사해 value serializer 만 바꾼다.
     * 빈으로 등록하지 않는다: KafkaTemplate·ProducerFactory 빈을 더 두면 Boot 자동 구성 빈이 빠진다.
     */
    private KafkaTemplate<String, byte[]> bytesTemplate(ProducerFactory<String, Object> producerFactory) {
        bytesProducerFactory = new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(), new StringSerializer(), new ByteArraySerializer());
        return new KafkaTemplate<>(bytesProducerFactory);
    }

    @Override
    public void destroy() {
        if (bytesProducerFactory != null) {
            bytesProducerFactory.destroy();
        }
    }
}
