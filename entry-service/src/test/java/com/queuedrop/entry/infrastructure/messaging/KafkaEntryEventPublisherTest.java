package com.queuedrop.entry.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.nio.charset.StandardCharsets;
import tools.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * entry.created 동기 발행 어댑터 통합 테스트 (단계 문서 4장 T4-23, T4-24).
 * T4-24 는 공유 컨텍스트의 브로커를 멈추지 않도록 전용 Kafka 컨테이너를 띄우고,
 * 컨텍스트의 producer 설정(application.yml)을 그대로 쓴 KafkaTemplate 으로 어댑터를 직접 만든다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        KafkaEntryEventPublisherTest.TopicConfig.class})
class KafkaEntryEventPublisherTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(15);

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        /** 자동 생성에 기대지 않고 KafkaAdmin 이 먼저 만든다 */
        @Bean
        NewTopic entryCreatedTopic() {
            return TopicBuilder.name(Topics.ENTRY_CREATED).partitions(1).replicas(1).build();
        }
    }

    @Autowired EntryEventPublisher publisher;
    @Autowired KafkaContainer kafka;
    @Autowired ProducerFactory<String, Object> producerFactory;
    @Autowired ConsumerFactory<String, Object> consumerFactory;
    @Value("${entry.publish.await-timeout}") Duration awaitTimeout;

    private KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "entry-created-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(Topics.ENTRY_CREATED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> key.equals(r.key())).toList();
    }

    @Test
    @DisplayName("[T4-23] 발행 포트 빈은 KafkaEntryEventPublisher 다")
    void publisherBeanIsKafkaAdapter() {
        // Then
        assertThat(AopUtils.getTargetClass(publisher)).isEqualTo(KafkaEntryEventPublisher.class);
    }

    @Test
    @DisplayName("[T4-23] publishCreated 는 반환 시점에 기록이 끝났고, entry.created 에 key={dropId}:{userId}, 필드 일치 메시지 1건이 수신된다")
    void publishCreatedSendsKeyedMessage() throws Exception {
        // Given
        long dropId = 4_000_000L + (System.nanoTime() % 100_000);
        long userId = 77L;
        String eventId = UUID.randomUUID().toString();
        Instant appliedAt = Instant.parse("2026-11-20T10:30:00.123456Z");
        EntryCreatedPayload payload = new EntryCreatedPayload(eventId, dropId, userId, 7, appliedAt);

        // When: 동기 발행 — 예외 없이 돌아오면 브로커 기록이 확인된 것
        publisher.publishCreated(payload);

        // Then
        String key = dropId + ":" + userId;
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> recordsWithKey(key).size() == 1);
        EntryCreated message = JSON.readValue(recordsWithKey(key).getFirst().value(), EntryCreated.class);
        assertThat(message).isEqualTo(new EntryCreated(eventId, dropId, userId, 7, appliedAt));
    }

    @Test
    @DisplayName("[T4-23] 메시지 JSON 은 common EntryCreated 필드명 5개, appliedAt ISO-8601 문자열, __TypeId__ 헤더=EntryCreated FQCN 이다")
    void messageWireFormatMatchesCommonContract() throws Exception {
        // Given
        long dropId = 4_100_000L + (System.nanoTime() % 100_000);
        String eventId = UUID.randomUUID().toString();
        Instant appliedAt = Instant.parse("2026-11-20T10:30:00.123456Z");

        // When
        publisher.publishCreated(new EntryCreatedPayload(eventId, dropId, 88L, 3, appliedAt));

        // Then
        String key = dropId + ":88";
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        ConsumerRecord<String, String> record = recordsWithKey(key).getFirst();

        Header typeId = record.headers().lastHeader("__TypeId__");
        assertThat(typeId).as("소비자 JacksonJsonDeserializer 가 타입을 찾는 헤더").isNotNull();
        assertThat(new String(typeId.value(), StandardCharsets.UTF_8)).isEqualTo(EntryCreated.class.getName());

        JsonNode json = JSON.readTree(record.value());
        assertThat(json.propertyNames()).containsExactlyInAnyOrder(
                "eventId", "dropId", "userId", "entrySeq", "appliedAt");
        assertThat(json.get("eventId").asString()).isEqualTo(eventId);
        assertThat(json.get("dropId").asLong()).isEqualTo(dropId);
        assertThat(json.get("entrySeq").asInt()).isEqualTo(3);
        assertThat(json.get("appliedAt").isString()).as("숫자 타임스탬프가 아니라 ISO-8601 문자열").isTrue();
        assertThat(Instant.parse(json.get("appliedAt").asString())).isEqualTo(appliedAt);
    }

    @Test
    @DisplayName("[T4-23] 컨텍스트의 소비자 설정(ErrorHandlingDeserializer→JacksonJsonDeserializer, trusted.packages)으로 EntryCreated 로 역직렬화된다")
    void contextConsumerDeserializesEntryCreated() {
        // Given
        long dropId = 4_200_000L + (System.nanoTime() % 100_000);
        String eventId = UUID.randomUUID().toString();
        Instant appliedAt = Instant.parse("2026-11-20T10:31:00Z");
        String key = dropId + ":99";
        List<Object> values = new ArrayList<>();

        try (Consumer<String, Object> typed = consumerFactory.createConsumer(
                "entry-created-typed-" + UUID.randomUUID(), null)) {
            typed.subscribe(List.of(Topics.ENTRY_CREATED));

            // When
            publisher.publishCreated(new EntryCreatedPayload(eventId, dropId, 99L, 5, appliedAt));

            // Then
            await().atMost(RECEIVE_TIMEOUT).until(() -> {
                typed.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) values.add(r.value());
                });
                return !values.isEmpty();
            });
        }
        assertThat(values.getFirst())
                .isEqualTo(new EntryCreated(eventId, dropId, 99L, 5, appliedAt));
    }

    @Test
    @DisplayName("[T4-24] Kafka 브로커가 정지되면 publishCreated 는 5초 안에 EventPublishException 을 던진다")
    void brokerDownThrowsEventPublishExceptionWithin5Seconds() {
        try (KafkaContainer dedicated = new KafkaContainer("apache/kafka:4.0.0")) {
            // Given: 전용 브로커 + application.yml 의 producer 설정 그대로(bootstrap 만 교체)
            dedicated.start();
            Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dedicated.getBootstrapServers());
            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(config);
            KafkaEntryEventPublisher adapter =
                    new KafkaEntryEventPublisher(new KafkaTemplate<>(factory), awaitTimeout);
            try {
                // Given: 정상 상태에서 1회 발행해 연결·메타데이터를 확보한 뒤 브로커 정지
                adapter.publishCreated(new EntryCreatedPayload(
                        UUID.randomUUID().toString(), 1L, 1L, 1, Instant.parse("2026-11-20T10:00:00Z")));
                dedicated.stop();
                EntryCreatedPayload payload = new EntryCreatedPayload(
                        UUID.randomUUID().toString(), 1L, 2L, 2, Instant.parse("2026-11-20T10:00:01Z"));

                // When
                long start = System.nanoTime();
                Throwable thrown = catchThrowable(
                        () -> adapter.publishCreated(payload));
                Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

                // Then
                assertThat(thrown).isInstanceOf(EventPublishException.class);
                assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
            } finally {
                factory.destroy();
            }
        }
    }

    @Test
    @DisplayName("[T4-24] await-timeout 설정은 delivery.timeout.ms(3000) 보다 길고 3.5초다")
    void awaitTimeoutIsConfigured() {
        // Then
        assertThat(awaitTimeout).isEqualTo(Duration.ofMillis(3500));
        assertThat(producerFactory.getConfigurationProperties())
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000")
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000")
                .containsEntry(ProducerConfig.LINGER_MS_CONFIG, "0");
    }
}
