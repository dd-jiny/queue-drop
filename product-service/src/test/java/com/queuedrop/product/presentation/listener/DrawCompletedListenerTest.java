package com.queuedrop.product.presentation.listener;

import static com.queuedrop.product.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.event.DrawCompleted;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.service.DropQueryService;
import com.queuedrop.product.support.FaultInjectingDropRepository;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/**
 * draw.completed 수신 (단계 문서 3.5, 4장 T5-25).
 * 토픽을 파티션 1개로 만들어, 나중에 보낸 "확인용" 메시지가 처리됐으면 앞선 메시지도 처리(재시도 포함)가 끝났다고 판단한다.
 * 처리 시도 수는 FaultInjectingDropRepository 의 findById 호출 수로 센다 (markDrawn 1회 = findById 1회).
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, KafkaContainerConfig.class,
        DrawCompletedListenerTest.TopicConfig.class, FaultInjectingDropRepository.Config.class})
class DrawCompletedListenerTest {

    private static final Duration WAIT = Duration.ofSeconds(15);
    /** 1초 간격 3회 재시도(약 3초) + DLT 발행 + 수신 여유 */
    private static final Duration DLT_WAIT = Duration.ofSeconds(25);
    /** 첫 시도 1회 + 재시도 3회 (entry KafkaConsumerConfig 와 같은 정책) */
    private static final int ATTEMPTS_WITH_RETRY = 4;

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaContainer kafka;
    @Autowired DropQueryService dropQueryService;
    @Autowired FaultInjectingDropRepository faults;

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        @Bean NewTopic drawCompletedTopic()    { return TopicBuilder.name(Topics.DRAW_COMPLETED).partitions(1).replicas(1).build(); }
        @Bean NewTopic drawCompletedDltTopic() { return TopicBuilder.name(Topics.DRAW_COMPLETED_DLT).partitions(1).replicas(1).build(); }
    }

    @BeforeEach
    void setUp() {
        faults.reset();
        // NewTopic 직후 첫 send 가 max.block.ms(1초) 안에 메타데이터를 못 받는 것을 막는다.
        // DLT 발행도 같은 공유 producer 를 쓰므로 DLT 토픽 메타데이터도 미리 받아 둔다 (첫 DLT send 실패 → 재시도 횟수 흔들림 방지)
        awaitTopicMetadata(kafkaTemplate, Topics.DRAW_COMPLETED, Topics.DRAW_COMPLETED_DLT);
    }

    @AfterEach
    void tearDown() {
        faults.reset();
    }

    /** 마감이 지난(CLOSED) drawn=false 드로우를 DB 에 직접 둔다 */
    private long givenUndrawnDrop() {
        Long productId = jdbc.queryForObject(
                "insert into products (name, created_at) values ('추첨 반영 상품', now()) returning id", Long.class);
        Instant openAt = Instant.now().minus(Duration.ofHours(3)).truncatedTo(ChronoUnit.MICROS);
        Instant closeAt = openAt.plus(Duration.ofHours(2));
        return jdbc.queryForObject(
                "insert into drops (product_id, title, entry_capacity, winner_count, open_at, close_at, created_at) "
                        + "values (?, ?, 100, 10, ?, ?, now()) returning id",
                Long.class, productId, "추첨-" + UUID.randomUUID(), Timestamp.from(openAt), Timestamp.from(closeAt));
    }

    private boolean drawn(long dropId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("select drawn from drops where id = ?", Boolean.class, dropId));
    }

    private void publish(long dropId) throws Exception {
        DrawCompleted message = new DrawCompleted(dropId, 120, 10, Instant.now().truncatedTo(ChronoUnit.MICROS));
        kafkaTemplate.send(Topics.DRAW_COMPLETED, String.valueOf(dropId), message).get(5, TimeUnit.SECONDS);
    }

    /** 확인용 드로우를 보내고 반영될 때까지 기다린다 → 파티션 1개라 앞선 메시지 처리도 끝났다 */
    private void publishSentinelAndAwait() throws Exception {
        long sentinel = givenUndrawnDrop();
        publish(sentinel);
        await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(100)).until(() -> drawn(sentinel));
    }

    @Test
    @DisplayName("[T5-25] drawn=false 드로우에 draw.completed 를 2번 받으면 drawn=true, 재시도·DLT 없이 처리되고 상세 상태는 DRAWN")
    void drawCompletedTwiceMarksDrawnIdempotently() throws Exception {
        // Given
        long dropId = givenUndrawnDrop();
        assertThat(dropQueryService.get(dropId).status()).isEqualTo("CLOSED");
        faults.reset();   // 위 상세 조회도 findById 를 거치므로, 리스너 처리 시도만 세도록 비운다

        try (var dlt = new DltReader(kafka.getBootstrapServers(), Topics.DRAW_COMPLETED_DLT)) {
            // When — 같은 메시지 2번 (중복 발행), 이어서 확인용 메시지
            publish(dropId);
            publish(dropId);
            publishSentinelAndAwait();

            // Then — 두 번 모두 1회씩 처리됐다 (예외 → 재시도였다면 호출이 더 많다)
            assertThat(drawn(dropId)).isTrue();
            assertThat(faults.findCalls(dropId)).isEqualTo(2);
            assertThat(dropQueryService.get(dropId).status()).isEqualTo("DRAWN");

            // Then — DLT 로 가지 않았다
            String key = String.valueOf(dropId);
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                    .until(() -> dlt.recordsWithKey(key).isEmpty());
        }
    }

    @Test
    @DisplayName("[T5-25] 없는 dropId 의 draw.completed 는 재시도·DLT 없이 커밋되고, 뒤이은 메시지는 정상 처리된다")
    void unknownDropIsCommittedWithoutRetryOrDlt() throws Exception {
        // Given
        long unknownId = 987_654_321L;
        String key = String.valueOf(unknownId);

        try (var dlt = new DltReader(kafka.getBootstrapServers(), Topics.DRAW_COMPLETED_DLT)) {
            // When
            publish(unknownId);
            publishSentinelAndAwait();

            // Then — 한 번만 시도했다 (PRODUCT_DROP_NOT_FOUND 는 비재시도)
            assertThat(faults.findCalls(unknownId)).isEqualTo(1);

            // Then — DLT 로 가지 않았다 (잠시 지켜봐도 0건)
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                    .until(() -> dlt.recordsWithKey(key).isEmpty());
            assertThat(faults.findCalls(unknownId)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("[T5-25] DROP_NOT_FOUND 가 아닌 예외(DB 장애 가정)는 재시도 후 draw.completed.DLT 로 가고 drawn 은 false 그대로다")
    void otherExceptionIsRetriedThenDlt() throws Exception {
        // Given
        long dropId = givenUndrawnDrop();
        faults.failOnFind(dropId, () -> new IllegalStateException("DB 장애 가정"));

        assertRetriedThenDlt(dropId);
    }

    @Test
    @DisplayName("[T5-25] DROP_NOT_FOUND 가 아닌 ApplicationException 은 비재시도가 아니다 — 재시도 후 draw.completed.DLT (E-30)")
    void otherApplicationExceptionIsRetriedThenDlt() throws Exception {
        // Given — ApplicationException 전체를 not-retryable 로 등록하면 이 테스트가 실패한다
        long dropId = givenUndrawnDrop();
        faults.failOnFind(dropId, () -> new ApplicationException(ProductErrorCode.INVALID_DROP));

        assertRetriedThenDlt(dropId);
    }

    @Test
    @DisplayName("[T5-25] 역직렬화 실패(JSON 아닌 바이트)는 재시도 없이 draw.completed.DLT 로 원본 바이트 그대로 가고, 뒤이은 메시지는 정상 처리된다")
    void poisonPillGoesToDltWithOriginalBytes() throws Exception {
        // Given — 커스텀 recoverer(DROP_NOT_FOUND 거름)를 거쳐도 byte[] 템플릿으로 원본이 보존되는지 본다
        String key = "poison:" + UUID.randomUUID();
        byte[] poison = new byte[] {0x00, (byte) 0xFF, 0x7B, 0x6E, 0x6F, 0x74, 0x2D, 0x6A, 0x73, 0x6F, 0x6E};

        try (var dlt = new KafkaConsumer<String, byte[]>(Map.of(
                     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                     ConsumerConfig.GROUP_ID_CONFIG, "dlt-bytes-reader-" + UUID.randomUUID(),
                     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                     ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                     ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
             var rawProducer = new KafkaProducer<String, byte[]>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            dlt.subscribe(List.of(Topics.DRAW_COMPLETED_DLT));
            List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();

            // When
            long sentAt = System.nanoTime();
            rawProducer.send(new ProducerRecord<>(Topics.DRAW_COMPLETED, key, poison)).get(5, TimeUnit.SECONDS);

            // Then — DLT 에 같은 key 1건, 값은 원본 바이트
            await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(100)).until(() -> {
                dlt.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.stream().anyMatch(r -> key.equals(r.key()));
            });
            long elapsedMs = Duration.ofNanos(System.nanoTime() - sentAt).toMillis();
            List<ConsumerRecord<String, byte[]>> records = received.stream().filter(r -> key.equals(r.key())).toList();
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().value()).as("DLT 값이 원본 바이트와 같아야 한다").isEqualTo(poison);

            // Then — 재시도 없이 바로 (1초 간격 3회 재시도면 최소 3초)
            assertThat(elapsedMs).as("재시도(최소 3초) 없이 DLT 로 가야 한다").isLessThan(2_500L);
            Header exception = records.getFirst().headers().lastHeader("kafka_dlt-exception-fqcn");
            assertThat(exception).isNotNull();
            assertThat(new String(exception.value(), StandardCharsets.UTF_8)).contains("DeserializationException");

            // Then — 파티션이 막히지 않았다
            publishSentinelAndAwait();
        }
    }

    private void assertRetriedThenDlt(long dropId) throws Exception {
        String key = String.valueOf(dropId);
        try (var dlt = new DltReader(kafka.getBootstrapServers(), Topics.DRAW_COMPLETED_DLT)) {
            // When
            publish(dropId);

            // Then — 재시도 후 DLT 에 같은 key 의 레코드 1건
            await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(200))
                    .until(() -> !dlt.recordsWithKey(key).isEmpty());
            assertThat(dlt.recordsWithKey(key)).hasSize(1);
            assertThat(faults.findCalls(dropId)).isEqualTo(ATTEMPTS_WITH_RETRY);
            assertThat(drawn(dropId)).isFalse();

            // Then — 장애가 풀리면 뒤이은 메시지는 정상 처리된다 (컨슈머가 막히지 않았다)
            faults.reset();
            publishSentinelAndAwait();
        }
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
