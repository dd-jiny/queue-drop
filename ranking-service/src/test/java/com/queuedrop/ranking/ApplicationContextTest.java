package com.queuedrop.ranking;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.ranking.support.KafkaTopicReadiness;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * T0-04: Redis·Kafka 컨테이너로 기동 + 실제 적용된 Kafka 클라이언트 설정 + Redis 연결 + DropCreated 직렬화 왕복.
 */
@SpringBootTest
@Import({RedisContainerConfig.class, KafkaContainerConfig.class})
class ApplicationContextTest {

    /** 운영 토픽(drop.created)을 쓰면 이후 단계의 리스너가 테스트 레코드를 소비할 수 있어 테스트 전용 토픽을 쓴다 */
    private static final String TEST_TOPIC = "t0-04.drop-created";

    @Autowired ProducerFactory<?, ?> producerFactory;
    @Autowired ConsumerFactory<?, ?> consumerFactory;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired RedisConnectionFactory redisConnectionFactory;

    @Value("${spring.application.name}")
    String applicationName;

    @Test
    @DisplayName("[T0-04] ranking-service 프로듀서에 acks=all·멱등·타임아웃·Jackson 직렬화가 적용된다")
    void producerConfigApplied() {
        // Given / When: 기동된 컨텍스트의 ProducerFactory 가 실제로 쓰는 설정
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        // Then
        assertThat(String.valueOf(config.get(ProducerConfig.ACKS_CONFIG))).isEqualTo("all");
        assertThat(String.valueOf(config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))).isEqualTo("true");
        assertThat(String.valueOf(config.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG))).isEqualTo("1000");
        assertThat(String.valueOf(config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))).isEqualTo("3000");
        assertThat(String.valueOf(config.get(ProducerConfig.RETRIES_CONFIG))).isEqualTo(String.valueOf(Integer.MAX_VALUE));
        assertThat(config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(JacksonJsonSerializer.class);
    }

    @Test
    @DisplayName("[T0-04] ranking-service 컨슈머에 ErrorHandlingDeserializer + Jackson delegate·trusted.packages·group.id 가 적용된다")
    void consumerConfigApplied() {
        // Given / When: 기동된 컨텍스트의 ConsumerFactory 가 실제로 쓰는 설정
        Map<String, Object> config = consumerFactory.getConfigurationProperties();

        // Then
        assertThat(config.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo(applicationName).isEqualTo("ranking-service");
        assertThat(config.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(config.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(config.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ErrorHandlingDeserializer.class);
        assertThat(String.valueOf(config.get(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS)))
                .isEqualTo(JacksonJsonDeserializer.class.getName());
        assertThat(String.valueOf(config.get(JacksonJsonDeserializer.TRUSTED_PACKAGES)))
                .isEqualTo("com.queuedrop.common.event");
    }

    @Test
    @DisplayName("[T0-04] ranking-service 가 @ServiceConnection 으로 연결된 Redis 에 PING 하면 PONG 을 받는다")
    void redisConnected() {
        // Given: RedisContainerConfig 의 @ServiceConnection(name = "redis")
        try (RedisConnection connection = redisConnectionFactory.getConnection()) {
            // When
            String pong = connection.ping();

            // Then
            assertThat(pong).isEqualTo("PONG");
        }
    }

    @Test
    @DisplayName("[T0-04] ranking-service 에서 DropCreated 를 동기 발행(3.5초)하고 같은 레코드를 DropCreated 로 역직렬화한다")
    void dropCreatedRoundTrip() throws Exception {
        // Given: 운영과 같이 자동 생성에 기대지 않고 토픽을 명시적으로 만든다
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_TOPIC).partitions(1).replicas(1).build());
        KafkaTopicReadiness.awaitTopicMetadata(kafkaTemplate, TEST_TOPIC);
        DropCreated event = new DropCreated(
                1L, 10L, "한정판 스니커즈", 1000, 100,
                Instant.parse("2026-12-01T10:00:00.123456Z"),
                Instant.parse("2026-12-01T11:00:00Z"));

        // When: 호출 측 대기 3.5초 정책 그대로 동기 발행
        SendResult<String, Object> sent = kafkaTemplate.send(TEST_TOPIC, "1", event).get(3500, TimeUnit.MILLISECONDS);

        // Then: 브로커에 기록됐고, 앱 설정 그대로의 컨슈머(별도 group)가 같은 레코드를 DropCreated 로 읽는다
        assertThat(sent.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
        try (Consumer<?, ?> consumer = consumerFactory.createConsumer("t0-04-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(TEST_TOPIC));
            ConsumerRecord<?, ?> received = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC, Duration.ofSeconds(10));

            // 기본 타입(spring.json.value.default.type)이 없으므로 타입 헤더로 DropCreated 가 결정된 것이다.
            // JacksonJsonDeserializer 는 역직렬화 후 타입 헤더를 제거하므로(removeTypeHeaders 기본 true) 헤더는 아래 raw 컨슈머로 확인한다.
            assertThat(received.key()).isEqualTo("1");
            assertThat(received.value()).isInstanceOf(DropCreated.class).isEqualTo(event);
        }

        // Then: 와이어 포맷 — 타입 헤더가 붙고, Instant 는 ISO-8601 문자열로 나간다
        Properties raw = new Properties();
        raw.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Consumer<?, ?> rawConsumer = consumerFactory.createConsumer("t0-04-raw-" + UUID.randomUUID(), null, null, raw)) {
            rawConsumer.subscribe(List.of(TEST_TOPIC));
            ConsumerRecord<?, ?> received = KafkaTestUtils.getSingleRecord(rawConsumer, TEST_TOPIC, Duration.ofSeconds(10));

            Header typeId = received.headers().lastHeader("__TypeId__");
            assertThat(typeId).isNotNull();
            assertThat(new String(typeId.value(), StandardCharsets.UTF_8)).isEqualTo(DropCreated.class.getName());
            assertThat((String) received.value())
                    .contains("\"openAt\":\"2026-12-01T10:00:00.123456Z\"")
                    .contains("\"closeAt\":\"2026-12-01T11:00:00Z\"");
        }
    }
}
