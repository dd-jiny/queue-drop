package com.queuedrop.entry.infrastructure.messaging;

import static com.queuedrop.entry.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.DrawCompleted;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.port.DrawEventPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * draw.completed 발행 어댑터 통합 테스트 (단계 문서 5단계 4장 T5-20).
 * 소비자는 product-service 이므로 wire format(필드명·Instant 문자열·__TypeId__)까지 고정한다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        KafkaDrawEventPublisherTest.TopicConfig.class})
class KafkaDrawEventPublisherTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(15);

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        /** 자동 생성에 기대지 않고 KafkaAdmin 이 먼저 만든다 */
        @Bean
        NewTopic drawCompletedTopic() {
            return TopicBuilder.name(Topics.DRAW_COMPLETED).partitions(1).replicas(1).build();
        }
    }

    @Autowired DrawEventPublisher publisher;
    @Autowired KafkaContainer kafka;
    @Autowired ConsumerFactory<String, Object> consumerFactory;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        // 어댑터가 쓰는 공유 producer 에 토픽 메타데이터를 먼저 적재 (첫 발행의 max.block.ms 1초 초과 방지)
        awaitTopicMetadata(kafkaTemplate, Topics.DRAW_COMPLETED);
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "draw-completed-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(Topics.DRAW_COMPLETED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> key.equals(r.key())).toList();
    }

    private static long uniqueDropId(long base) {
        return base + (System.nanoTime() % 100_000);
    }

    @Test
    @DisplayName("[T5-20] 발행 포트 빈은 KafkaDrawEventPublisher 다")
    void publisherBeanIsKafkaAdapter() {
        // Then
        assertThat(AopUtils.getTargetClass(publisher)).isEqualTo(KafkaDrawEventPublisher.class);
    }

    @Test
    @DisplayName("[T5-20] publishCompleted 후 draw.completed 에 키=dropId, 필드가 payload 와 같은 DrawCompleted 메시지 정확히 1건이 수신된다")
    void publishCompletedSendsKeyedMessage() throws Exception {
        // Given
        long dropId = uniqueDropId(5_000_000L);
        Instant drawnAt = Instant.parse("2026-11-20T11:00:05.123456Z");
        DrawCompletedPayload payload = new DrawCompletedPayload(dropId, 120, 10, drawnAt);

        // When — 동기 발행: 예외 없이 돌아오면 브로커 기록 확인
        publisher.publishCompleted(payload);

        // Then
        String key = String.valueOf(dropId);
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> recordsWithKey(key).size() == 1);
        DrawCompleted message = JSON.readValue(recordsWithKey(key).getFirst().value(), DrawCompleted.class);
        assertThat(message).isEqualTo(new DrawCompleted(dropId, 120, 10, drawnAt));
    }

    @Test
    @DisplayName("[T5-20] 메시지 JSON 은 common DrawCompleted 필드명 4개, drawnAt ISO-8601 문자열, __TypeId__ 헤더=DrawCompleted FQCN 이다")
    void messageWireFormatMatchesCommonContract() throws Exception {
        // Given — applicantCount ≠ winnerCount 로 필드 뒤바뀜을 구분
        long dropId = uniqueDropId(5_100_000L);
        Instant drawnAt = Instant.parse("2026-11-20T11:00:06Z");

        // When
        publisher.publishCompleted(new DrawCompletedPayload(dropId, 7, 3, drawnAt));

        // Then
        String key = String.valueOf(dropId);
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        ConsumerRecord<String, String> record = recordsWithKey(key).getFirst();

        Header typeId = record.headers().lastHeader("__TypeId__");
        assertThat(typeId).as("소비자 JacksonJsonDeserializer 가 타입을 찾는 헤더").isNotNull();
        assertThat(new String(typeId.value(), StandardCharsets.UTF_8)).isEqualTo(DrawCompleted.class.getName());

        JsonNode json = JSON.readTree(record.value());
        assertThat(json.propertyNames()).containsExactlyInAnyOrder(
                "dropId", "applicantCount", "winnerCount", "drawnAt");
        assertThat(json.get("dropId").asLong()).isEqualTo(dropId);
        assertThat(json.get("applicantCount").asInt()).isEqualTo(7);
        assertThat(json.get("winnerCount").asInt()).isEqualTo(3);
        assertThat(json.get("drawnAt").isString()).as("숫자 타임스탬프가 아니라 ISO-8601 문자열").isTrue();
        assertThat(Instant.parse(json.get("drawnAt").asString())).isEqualTo(drawnAt);
    }

    @Test
    @DisplayName("[T5-20] (경계) 응모 0건 결과(0, 0)도 발행되고, 컨텍스트 소비자 설정으로 DrawCompleted 로 역직렬화된다")
    void zeroApplicantsIsPublishedAndDeserializable() {
        // Given
        long dropId = uniqueDropId(5_200_000L);
        Instant drawnAt = Instant.parse("2026-11-20T11:00:07Z");
        String key = String.valueOf(dropId);
        List<Object> values = new ArrayList<>();

        try (Consumer<String, Object> typed = consumerFactory.createConsumer(
                "draw-completed-typed-" + UUID.randomUUID(), null)) {
            typed.subscribe(List.of(Topics.DRAW_COMPLETED));

            // When
            publisher.publishCompleted(new DrawCompletedPayload(dropId, 0, 0, drawnAt));

            // Then
            await().atMost(RECEIVE_TIMEOUT).until(() -> {
                typed.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) values.add(r.value());
                });
                return !values.isEmpty();
            });
        }
        assertThat(values.getFirst()).isEqualTo(new DrawCompleted(dropId, 0, 0, drawnAt));
    }
}
