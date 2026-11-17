package com.queuedrop.entry.presentation.listener;

import com.queuedrop.common.event.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

/**
 * entry-service 리스너 통합 테스트 공통 지원 (단계 문서 4.2 통합 테스트 설정).
 * 두 테스트 클래스가 같은 @Import 조합을 써서 Spring 컨텍스트(와 컨테이너)를 공유한다.
 */
final class ListenerTestSupport {

    private ListenerTestSupport() {}

    /**
     * 리스너 컨테이너 기동 전에 KafkaAdmin 이 토픽을 만든다 (queue DropCreatedListenerIntegrationTest 와 같음).
     * 모든 토픽을 파티션 1개로 만들어, 나중에 보낸 "확인용" 메시지가 처리됐으면 앞선 메시지도 처리됐다고 판단한다.
     * DLT 는 원본과 같은 파티션 수여야 한다 (기본 recoverer 가 원본 파티션 번호로 보낸다).
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        @Bean NewTopic dropCreatedTopic()    { return TopicBuilder.name(Topics.DROP_CREATED).partitions(1).replicas(1).build(); }
        @Bean NewTopic entryCreatedTopic()   { return TopicBuilder.name(Topics.ENTRY_CREATED).partitions(1).replicas(1).build(); }
        @Bean NewTopic dropCreatedDltTopic() { return TopicBuilder.name(Topics.DROP_CREATED_DLT).partitions(1).replicas(1).build(); }
        @Bean NewTopic entryCreatedDltTopic(){ return TopicBuilder.name(Topics.ENTRY_CREATED_DLT).partitions(1).replicas(1).build(); }
    }

    /** DLT 를 문자열로 읽는 독립 소비자 (앱 그룹과 다른 group-id, earliest). key 로 걸러서 본다 */
    static final class DltReader implements AutoCloseable {

        private final KafkaConsumer<String, String> consumer;
        private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

        DltReader(String bootstrapServers, String topic) {
            consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
            consumer.subscribe(List.of(topic));
        }

        List<ConsumerRecord<String, String>> recordsWithKey(String key) {
            consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            return received.stream().filter(r -> key.equals(r.key())).toList();
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
