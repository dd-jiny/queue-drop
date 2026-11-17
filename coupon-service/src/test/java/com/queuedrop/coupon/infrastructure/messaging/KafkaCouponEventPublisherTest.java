package com.queuedrop.coupon.infrastructure.messaging;

import static com.queuedrop.coupon.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.coupon.application.port.CouponEventPublisher;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * coupon.issued 동기 발행 어댑터 통합 테스트 (단계 문서 4장 T6-12) + producer·consumer·listener 설정 (3.3).
 * 전체 컨텍스트를 띄우므로 모든 포트 구현체와 KafkaConsumerConfig 빈이 함께 기동되는지도 확인한다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        KafkaCouponEventPublisherTest.TopicConfig.class})
class KafkaCouponEventPublisherTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(15);

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        /** 자동 생성에 기대지 않고 KafkaAdmin 이 먼저 만든다 */
        @Bean
        NewTopic couponIssuedTopic() {
            return TopicBuilder.name(Topics.COUPON_ISSUED).partitions(1).replicas(1).build();
        }
    }

    @Autowired CouponEventPublisher publisher;
    @Autowired KafkaContainer kafka;
    @Autowired ProducerFactory<String, Object> producerFactory;
    @Autowired ConsumerFactory<String, Object> consumerFactory;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired KafkaProperties kafkaProperties;
    @Autowired ApplicationContext context;
    @Value("${coupon.publish.await-timeout}") Duration awaitTimeout;

    private KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        // 공유 producer 에 토픽 메타데이터를 먼저 적재 (첫 발행의 max.block.ms 1초 초과 방지)
        awaitTopicMetadata(kafkaTemplate, Topics.COUPON_ISSUED);
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "coupon-issued-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(Topics.COUPON_ISSUED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> key.equals(r.key())).toList();
    }

    private static long uniqueEventId(long base) {
        return base + (System.nanoTime() % 100_000);
    }

    @Test
    @DisplayName("[T6-12] 발행 포트 빈은 KafkaCouponEventPublisher 다")
    void publisherBeanIsKafkaAdapter() {
        // Then
        assertThat(AopUtils.getTargetClass(publisher)).isEqualTo(KafkaCouponEventPublisher.class);
    }

    @Test
    @DisplayName("[T6-12] publishIssued 후 coupon.issued 에 key={couponEventId}:{userId}, 필드 일치 메시지 1건이 수신된다")
    void publishIssuedSendsKeyedMessage() throws Exception {
        // Given
        long couponEventId = uniqueEventId(6_000_000L);
        long userId = 77L;
        String requestId = UUID.randomUUID().toString();
        Instant issuedAt = Instant.parse("2026-11-20T10:30:00.123456Z");

        // When: 동기 발행 — 예외 없이 돌아오면 브로커 기록이 확인된 것
        publisher.publishIssued(new CouponIssuedPayload(requestId, couponEventId, userId, issuedAt));

        // Then
        String key = couponEventId + ":" + userId;
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> recordsWithKey(key).size() == 1);
        CouponIssued message = JSON.readValue(recordsWithKey(key).getFirst().value(), CouponIssued.class);
        assertThat(message).isEqualTo(new CouponIssued(requestId, couponEventId, userId, issuedAt));
    }

    @Test
    @DisplayName("[T6-12] 메시지 JSON 은 common CouponIssued 필드명 4개, issuedAt ISO-8601 문자열, __TypeId__ 헤더=CouponIssued FQCN 이다")
    void messageWireFormatMatchesCommonContract() throws Exception {
        // Given
        long couponEventId = uniqueEventId(6_100_000L);
        String requestId = UUID.randomUUID().toString();
        Instant issuedAt = Instant.parse("2026-11-20T10:30:00.123456Z");

        // When
        publisher.publishIssued(new CouponIssuedPayload(requestId, couponEventId, 88L, issuedAt));

        // Then
        String key = couponEventId + ":88";
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        ConsumerRecord<String, String> record = recordsWithKey(key).getFirst();

        Header typeId = record.headers().lastHeader("__TypeId__");
        assertThat(typeId).as("소비자 JacksonJsonDeserializer 가 타입을 찾는 헤더").isNotNull();
        assertThat(new String(typeId.value(), StandardCharsets.UTF_8)).isEqualTo(CouponIssued.class.getName());

        JsonNode json = JSON.readTree(record.value());
        assertThat(json.propertyNames()).containsExactlyInAnyOrder("eventId", "couponEventId", "userId", "issuedAt");
        assertThat(json.get("eventId").asString()).isEqualTo(requestId);
        assertThat(json.get("couponEventId").asLong()).isEqualTo(couponEventId);
        assertThat(json.get("userId").asLong()).isEqualTo(88L);
        assertThat(json.get("issuedAt").isString()).as("숫자 타임스탬프가 아니라 ISO-8601 문자열").isTrue();
        assertThat(Instant.parse(json.get("issuedAt").asString())).isEqualTo(issuedAt);
    }

    @Test
    @DisplayName("[T6-12] 컨텍스트의 소비자 설정(ErrorHandlingDeserializer→JacksonJsonDeserializer, trusted.packages)으로 CouponIssued 로 역직렬화된다")
    void contextConsumerDeserializesCouponIssued() {
        // Given
        long couponEventId = uniqueEventId(6_200_000L);
        String requestId = UUID.randomUUID().toString();
        Instant issuedAt = Instant.parse("2026-11-20T10:31:00Z");
        String key = couponEventId + ":99";
        List<Object> values = new ArrayList<>();

        try (Consumer<String, Object> typed = consumerFactory.createConsumer(
                "coupon-issued-typed-" + UUID.randomUUID(), null)) {
            typed.subscribe(List.of(Topics.COUPON_ISSUED));

            // When
            publisher.publishIssued(new CouponIssuedPayload(requestId, couponEventId, 99L, issuedAt));

            // Then
            await().atMost(RECEIVE_TIMEOUT).until(() -> {
                typed.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) values.add(r.value());
                });
                return !values.isEmpty();
            });
        }
        assertThat(values.getFirst()).isEqualTo(new CouponIssued(requestId, couponEventId, 99L, issuedAt));
    }

    @Test
    @DisplayName("[T6-12] producer 설정: await-timeout 3.5초 > delivery.timeout 3000, acks=all, 멱등, request.timeout 1000, max.block 1000, linger 0")
    void producerSettings() {
        // Then
        assertThat(awaitTimeout).isEqualTo(Duration.ofMillis(3500));
        assertThat(producerFactory.getConfigurationProperties())
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000")
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000")
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000")
                .containsEntry(ProducerConfig.LINGER_MS_CONFIG, "0");
        assertThat(String.valueOf(producerFactory.getConfigurationProperties().get(ProducerConfig.RETRIES_CONFIG)))
                .isEqualTo(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("[T6-12] consumer·listener 설정: group-id=coupon-service, earliest, 자동 커밋 끔, ack-mode record, concurrency 3")
    void consumerAndListenerSettings() {
        // Then
        assertThat(kafkaProperties.getConsumer().getGroupId()).isEqualTo("coupon-service");
        assertThat(kafkaProperties.getConsumer().getAutoOffsetReset()).isEqualTo("earliest");
        assertThat(kafkaProperties.getConsumer().getEnableAutoCommit()).isFalse();
        assertThat(kafkaProperties.getListener().getAckMode()).isEqualTo(AckMode.RECORD);
        assertThat(kafkaProperties.getListener().getConcurrency()).isEqualTo(3);
    }

    @Test
    @DisplayName("[T6-12] KafkaConsumerConfig 의 DefaultErrorHandler 가 유일한 CommonErrorHandler 이고, KafkaTemplate·ProducerFactory 빈은 Boot 자동 구성 1개씩이다")
    void errorHandlerBeanWithoutDisplacingAutoConfiguredKafkaBeans() {
        // Then: DLT 전용 템플릿·팩토리를 빈으로 두면 자동 구성 빈이 빠진다 (3.3)
        assertThat(context.getBeansOfType(CommonErrorHandler.class)).hasSize(1)
                .allSatisfy((name, handler) -> assertThat(handler).isInstanceOf(DefaultErrorHandler.class));
        assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
        assertThat(context.getBeansOfType(ProducerFactory.class)).hasSize(1);
    }
}
