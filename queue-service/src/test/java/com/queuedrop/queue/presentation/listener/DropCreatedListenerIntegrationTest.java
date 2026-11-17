package com.queuedrop.queue.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.presentation.scheduler.AdmissionScheduler;
import com.queuedrop.queue.support.KafkaTopicReadiness;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * drop.created 수신 → queue meta Hash·active-drops 등록 (T3-22).
 * 토픽은 파티션 1개로 만들어, 나중에 보낸 "확인용" 메시지가 처리됐으면 앞선 메시지도 처리됐다고 판단한다.
 */
@SpringBootTest(properties = "queue.admission.scheduler-enabled=false")
@Import({RedisContainerConfig.class, KafkaContainerConfig.class,
        DropCreatedListenerIntegrationTest.TopicConfig.class})
class DropCreatedListenerIntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(15);

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        /** 리스너 컨테이너 기동 전에 KafkaAdmin 이 만든다 — 자동 생성·메타데이터 갱신 지연에 기대지 않는다 */
        @Bean
        NewTopic dropCreatedTopic() {
            return TopicBuilder.name(Topics.DROP_CREATED).partitions(1).replicas(1).build();
        }
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired ProducerFactory<String, Object> producerFactory;
    @Autowired StringRedisTemplate redis;
    @Autowired ApplicationContext context;

    /** Redis 만료가 실제 시계로 동작하므로 기준은 실제 현재 시각 (meta 만료 = closeAt + 1일) */
    private Instant base;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        base = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    }

    private void publish(DropCreated event) throws Exception {
        KafkaTopicReadiness.awaitTopicMetadata(kafkaTemplate, Topics.DROP_CREATED);
        kafkaTemplate.send(Topics.DROP_CREATED, String.valueOf(event.dropId()), event).get(5, TimeUnit.SECONDS);
    }

    private DropCreated event(Long dropId, Instant openAt, Instant closeAt) {
        return new DropCreated(dropId, 3L, "한정판 스니커즈", 100, 10, openAt, closeAt);
    }

    private Map<Object, Object> meta(Long dropId) {
        return redis.opsForHash().entries("queue:" + dropId + ":meta");
    }

    private void awaitMeta(Long dropId) {
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100)).until(() -> !meta(dropId).isEmpty());
    }

    private Double activeScore(Long dropId) {
        return redis.opsForZSet().score("queue:active-drops", String.valueOf(dropId));
    }

    @Test
    @DisplayName("[T3-22] drop.created 를 받으면 meta Hash(openAt·closeAt epoch ms, µs 는 내림)와 active-drops(score=closeAt)가 등록된다")
    void registersMeta() throws Exception {
        // Given — µs 정밀도 시각 (DropCreated 는 µs)
        Instant openAt = base.plus(Duration.ofHours(1)).plusNanos(123_456_000);
        Instant closeAt = base.plus(Duration.ofHours(3)).plusNanos(999_999_000);
        long openMs = base.plus(Duration.ofHours(1)).toEpochMilli() + 123;
        long closeMs = base.plus(Duration.ofHours(3)).toEpochMilli() + 999;

        // When
        publish(event(101L, openAt, closeAt));

        // Then
        awaitMeta(101L);
        assertThat(meta(101L)).containsEntry("openAt", String.valueOf(openMs))
                .containsEntry("closeAt", String.valueOf(closeMs));
        assertThat(activeScore(101L)).isEqualTo((double) closeMs);
    }

    @Test
    @DisplayName("[T3-22] 같은 메시지를 다시 받아도 값이 같고, 이미 있던 waiting·seq 는 유지된다 (멱등)")
    void redeliveryIsIdempotent() throws Exception {
        // Given — 1차 수신 후 사용자 2명이 줄을 선 상태
        DropCreated first = event(102L, base.plus(Duration.ofHours(1)), base.plus(Duration.ofHours(3)));
        publish(first);
        awaitMeta(102L);
        Map<Object, Object> metaBefore = meta(102L);
        redis.opsForZSet().add("queue:102:waiting", "7", 1);
        redis.opsForZSet().add("queue:102:waiting", "8", 2);
        redis.opsForValue().set("queue:102:seq", "2");

        // When — 같은 메시지 재수신, 이어서 확인용 메시지
        publish(first);
        publish(event(103L, base.plus(Duration.ofHours(1)), base.plus(Duration.ofHours(3))));
        awaitMeta(103L);

        // Then
        assertThat(meta(102L)).isEqualTo(metaBefore);
        assertThat(activeScore(102L)).isEqualTo((double) base.plus(Duration.ofHours(3)).toEpochMilli());
        assertThat(redis.opsForZSet().range("queue:102:waiting", 0, -1)).containsExactly("7", "8");
        assertThat(redis.opsForValue().get("queue:102:seq")).isEqualTo("2");
    }

    @Test
    @DisplayName("[T3-22] 불변식을 어긴 메시지(openAt ≥ closeAt)는 저장하지 않고, 컨슈머가 막히지 않아 다음 메시지는 처리된다")
    void invalidMessageIsSkipped() throws Exception {
        // Given
        Instant at = base.plus(Duration.ofHours(1));

        // When — 잘못된 메시지 2건(같음, 역전) 다음에 정상 메시지
        publish(event(104L, at, at));
        publish(event(105L, at.plus(Duration.ofHours(1)), at));
        publish(event(106L, at, at.plus(Duration.ofHours(2))));

        // Then
        awaitMeta(106L);
        assertThat(meta(104L)).isEmpty();
        assertThat(meta(105L)).isEmpty();
        assertThat(activeScore(104L)).isNull();
        assertThat(activeScore(105L)).isNull();
    }

    @Test
    @DisplayName("[T3-22] 필수 값이 null 인 메시지(openAt·closeAt 누락)는 저장하지 않고 다음 메시지는 처리된다")
    void nullFieldMessageIsSkipped() throws Exception {
        // Given
        Instant at = base.plus(Duration.ofHours(1));

        // When
        publish(event(107L, null, at));
        publish(event(108L, at, null));
        publish(event(109L, at, at.plus(Duration.ofHours(2))));

        // Then
        awaitMeta(109L);
        assertThat(meta(107L)).isEmpty();
        assertThat(meta(108L)).isEmpty();
        assertThat(activeScore(107L)).isNull();
        assertThat(activeScore(108L)).isNull();
    }

    @Test
    @DisplayName("[T3-22] 역직렬화할 수 없는 레코드(JSON 아님)는 건너뛰고 컨슈머가 막히지 않는다 (ErrorHandlingDeserializer)")
    void poisonPillDoesNotBlockConsumer() throws Exception {
        // Given — 같은 앱 설정에 값 직렬화만 String 으로 바꾼 프로듀서 (타입 헤더 없음)
        @SuppressWarnings({"unchecked", "rawtypes"})
        KafkaTemplate<String, String> rawTemplate = new KafkaTemplate<>((ProducerFactory) producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        Instant at = base.plus(Duration.ofHours(1));

        // 설정을 덮어쓴 템플릿은 별도 producer 라 메타데이터를 따로 받는다
        KafkaTopicReadiness.awaitTopicMetadata(rawTemplate, Topics.DROP_CREATED);

        // When
        rawTemplate.send(Topics.DROP_CREATED, "110", "not-a-json").get(5, TimeUnit.SECONDS);
        publish(event(111L, at, at.plus(Duration.ofHours(2))));

        // Then
        awaitMeta(111L);
        assertThat(meta(110L)).isEmpty();
    }

    @Test
    @DisplayName("[T3-22] scheduler-enabled=false 로 띄우면 AdmissionScheduler 빈이 없다 (리스너는 동작)")
    void schedulerDisabledInThisContext() {
        // Then
        assertThat(context.getBeanNamesForType(AdmissionScheduler.class)).isEmpty();
        assertThat(context.getBeanNamesForType(DropCreatedListener.class)).hasSize(1);
    }
}
