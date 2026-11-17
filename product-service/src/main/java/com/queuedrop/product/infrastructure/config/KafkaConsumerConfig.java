package com.queuedrop.product.infrastructure.config;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.product.application.exception.ProductErrorCode;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 컨슈머 실패 정책 (architecture.md 4장, 5단계 3.5): 1초 간격 3회 재시도 후 {topic}.DLT (entry 와 같음).
 * 예외: PRODUCT_DROP_NOT_FOUND 는 재시도해도 해결되지 않으므로 재시도 0회, recoverer 에서 걸러 ERROR 로그 후 커밋한다 (DLT 로 보내지 않음).
 * ApplicationException 전체를 not-retryable 로 등록하지 않는다 — 에러 코드로만 가른다 (E-30).
 * 커밋: ack-mode record 에서 recoverer 가 정상 반환하면 DefaultErrorHandler 의 ackAfterHandle(기본 true)로 컨테이너가
 * 그 레코드 오프셋을 바로 커밋한다. commitRecovered 는 MANUAL_IMMEDIATE 에서만 쓰이므로 설정하지 않는다.
 * Spring Kafka 4 의 기본 DLT 이름은 {topic}-dlt 라서, Topics.*_DLT({topic}.DLT) 로 보내도록 목적지를 명시한다.
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig implements DisposableBean {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;
    static final String DLT_SUFFIX = ".DLT";

    private static final BackOff RETRY = new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES);
    private static final BackOff NO_RETRY = new FixedBackOff(0L, 0L);

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
        var deadLetter = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            if (isDropNotFound(exception)) {
                logDropNotFound(record, exception);
                return;                                   // 커밋 (DLT 로 보내지 않음)
            }
            deadLetter.accept(record, exception);
        };

        var handler = new DefaultErrorHandler(recoverer, RETRY);
        handler.setBackOffFunction((record, exception) -> isDropNotFound(exception) ? NO_RETRY : RETRY);
        return handler;
    }

    /** 리스너 예외는 ListenerExecutionFailedException 으로 감싸져 오므로 원인 사슬에서 찾는다 */
    static boolean isDropNotFound(Throwable exception) {
        // 방문 집합으로 순환 원인 사슬(A → B → A)에서 끝나지 않는 순회를 막는다
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = exception; t != null && visited.add(t); t = t.getCause()) {
            if (t instanceof ApplicationException e && e.errorCode() == ProductErrorCode.DROP_NOT_FOUND) {
                return true;
            }
        }
        return false;
    }

    private static void logDropNotFound(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("드로우가 없어 메시지를 건너뜀 (재시도·DLT 없음) code={} topic={} partition={} offset={} key={}",
                ProductErrorCode.DROP_NOT_FOUND.code(), record.topic(), record.partition(), record.offset(),
                record.key(), exception);
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
