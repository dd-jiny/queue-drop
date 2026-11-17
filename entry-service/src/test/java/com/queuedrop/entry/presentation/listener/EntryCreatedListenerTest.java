package com.queuedrop.entry.presentation.listener;

import static com.queuedrop.entry.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * entry.created 저장 리스너 통합 테스트 (단계 문서 4장 T4-26, T4-34).
 * presentation 테스트라 domain·infrastructure 타입을 쓰지 않는다 — DB 는 JdbcTemplate, Redis 는 키 문자열로 직접 본다.
 * v1.5: 저장 컨슈머는 claim Lua 로 applicants + requests[userId] == eventId 를 확인하고 persisted 에 기록한다.
 * 응모자 등록은 entry-apply.lua 의 OK 결과와 같은 상태(SADD applicants + HSET requests)를 키로 직접 만든다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        ListenerTestSupport.TopicConfig.class})
class EntryCreatedListenerTest {

    private static final Duration WAIT = Duration.ofSeconds(15);
    /** 1초 간격 3회 재시도(약 3초) + DLT 발행 + 수신 여유 */
    private static final Duration DLT_WAIT = Duration.ofSeconds(25);
    private static final Instant APPLIED_AT = Instant.parse("2026-11-20T10:30:00.123456Z");

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired StringRedisTemplate redis;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaContainer kafka;
    @Autowired KafkaListenerEndpointRegistry registry;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE entries, drop_snapshots");
        // NewTopic 직후 첫 send 가 max.block.ms(1초) 안에 메타데이터를 못 받는 것을 막는다
        awaitTopicMetadata(kafkaTemplate, Topics.ENTRY_CREATED);
    }

    /** entry-apply.lua 가 OK 일 때 남기는 상태와 같다: applicants 에 userId, requests[userId] = eventId */
    private void registerApplicant(long dropId, long userId, String eventId) {
        redis.opsForSet().add("entry:" + dropId + ":applicants", String.valueOf(userId));
        redis.opsForHash().put("entry:" + dropId + ":requests", String.valueOf(userId), eventId);
    }

    private void registerApplicant(long dropId, EntryCreated event) {
        registerApplicant(dropId, event.userId(), event.eventId());
    }

    private boolean isPersisted(long dropId, long userId) {
        return Boolean.TRUE.equals(
                redis.opsForSet().isMember("entry:" + dropId + ":persisted", String.valueOf(userId)));
    }

    private void publish(EntryCreated event) throws Exception {
        kafkaTemplate.send(Topics.ENTRY_CREATED, event.dropId() + ":" + event.userId(), event)
                .get(5, TimeUnit.SECONDS);
    }

    private EntryCreated event(long dropId, long userId, int seq) {
        return new EntryCreated(UUID.randomUUID().toString(), dropId, userId, seq, APPLIED_AT);
    }

    private long countEntries(long dropId, long userId) {
        return jdbc.queryForObject("SELECT count(*) FROM entries WHERE drop_id = ? AND user_id = ?",
                Long.class, dropId, userId);
    }

    private long countEntries(long dropId) {
        return jdbc.queryForObject("SELECT count(*) FROM entries WHERE drop_id = ?", Long.class, dropId);
    }

    @Test
    @DisplayName("[T4-26] 같은 EntryCreated 를 2번 받아도 DB 에는 1건만 남고 필드가 메시지와 같다 (컨슈머 멱등)")
    void duplicateDeliveryKeepsSingleRow() throws Exception {
        // Given — 응모자 등록 (applicants + requests 에 메시지와 같은 eventId), 확인용 사용자도 등록
        long dropId = 1L;
        EntryCreated message = event(dropId, 10L, 1);
        EntryCreated probe = event(dropId, 11L, 2);
        registerApplicant(dropId, message);
        registerApplicant(dropId, probe);

        // When — 같은 이벤트(eventId 포함 동일) 2번, 이어서 확인용 이벤트 (파티션 1개라 순서대로 처리)
        publish(message);
        publish(message);
        publish(probe);

        // Then — 확인용 이벤트가 저장됐으면 앞선 두 건도 처리가 끝난 것
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> countEntries(dropId, 11L) == 1);
        assertThat(countEntries(dropId, 10L)).isEqualTo(1);
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT entry_seq, status, applied_at FROM entries WHERE drop_id = ? AND user_id = ?",
                dropId, 10L);
        assertThat(row.get("entry_seq")).isEqualTo(1);
        assertThat(row.get("status")).isEqualTo("APPLIED");
        assertThat(((Timestamp) row.get("applied_at")).toInstant()).isEqualTo(APPLIED_AT);
        // v1.5: claim Lua 가 저장 전에 persisted 에 확정했다 (이후 보상은 ALREADY_PERSISTED)
        assertThat(isPersisted(dropId, 10L)).isTrue();
        assertThat(isPersisted(dropId, 11L)).isTrue();
        assertThat(redis.opsForHash().get("entry:" + dropId + ":requests", "10")).isEqualTo(message.eventId());
    }

    @Test
    @DisplayName("[T4-26] 응모자지만 requests 의 eventId 가 메시지와 다르면(재응모 전 시도의 유령 이벤트) 저장하지 않고 persisted 에도 넣지 않는다")
    void ghostEventOfPreviousAttemptIsSkipped() throws Exception {
        // Given — user 30 은 다른 요청(e-current)으로 응모자. 확인용 user 31 은 정상
        long dropId = 4L;
        registerApplicant(dropId, 30L, "e-current");
        EntryCreated ghost = event(dropId, 30L, 1);                  // eventId 는 랜덤 UUID → 불일치
        EntryCreated probe = event(dropId, 31L, 2);
        registerApplicant(dropId, probe);

        // When
        publish(ghost);
        publish(probe);

        // Then — 확인용 이벤트가 저장됐으면 유령 이벤트도 처리가 끝난 것
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> countEntries(dropId, 31L) == 1);
        assertThat(countEntries(dropId, 30L)).isZero();
        assertThat(isPersisted(dropId, 30L)).isFalse();
        assertThat(redis.opsForSet().isMember("entry:" + dropId + ":applicants", "30")).isTrue();
    }

    @Test
    @DisplayName("[T4-26] 저장 리스너 컨테이너는 id=entry-saver 로 등록되고 group-id 는 entry-service 그대로다 (idIsGroup=false)")
    void saverContainerIdAndGroup() {
        // When
        MessageListenerContainer container = registry.getListenerContainer("entry-saver");

        // Then
        assertThat(container).isNotNull();
        assertThat(container.getGroupId()).isEqualTo("entry-service");
    }

    @Test
    @DisplayName("[T4-34] entry_seq 충돌로 저장이 계속 실패하면 재시도 후 entry.created.DLT 로 같은 key 의 레코드가 가고, DB 는 1건 그대로다")
    void seqConflictGoesToDlt() throws Exception {
        // Given — (drop 2, user 1, seq 1) 이 이미 저장돼 있고, user 2 는 Redis 응모자
        //         (v1.5: requests 에 메시지와 같은 eventId → claim 은 통과하고 INSERT 에서 실패)
        long dropId = 2L;
        jdbc.update("INSERT INTO entries (drop_id, user_id, entry_seq, status, applied_at) VALUES (?, ?, ?, ?, ?)",
                dropId, 1L, 1, "APPLIED", Timestamp.from(APPLIED_AT));
        registerApplicant(dropId, 1L, "e-user1");
        EntryCreated conflicting = event(dropId, 2L, 1);
        registerApplicant(dropId, conflicting);
        String key = dropId + ":2";

        try (var dlt = new ListenerTestSupport.DltReader(kafka.getBootstrapServers(), Topics.ENTRY_CREATED_DLT)) {
            // When — user 2 가 같은 seq 1 로 들어온다 (uk_entries_drop_seq 위반 → DataIntegrityViolationException)
            publish(conflicting);

            // Then — 재시도(3회) 후 DLT 수신
            await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(200))
                    .until(() -> !dlt.recordsWithKey(key).isEmpty());
            List<ConsumerRecord<String, String>> records = dlt.recordsWithKey(key);
            assertThat(records).hasSize(1);
            Header originalTopic = records.getFirst().headers().lastHeader("kafka_dlt-original-topic");
            assertThat(originalTopic).as("DeadLetterPublishingRecoverer 가 붙이는 원본 토픽 헤더").isNotNull();
            assertThat(new String(originalTopic.value(), StandardCharsets.UTF_8)).isEqualTo(Topics.ENTRY_CREATED);
        }
        assertThat(countEntries(dropId)).isEqualTo(1);
        assertThat(countEntries(dropId, 2L)).isZero();
        // v1.5: claim 은 INSERT 전에 성공했으므로 persisted 에 남는다 (문서 3.3 "남는 불일치": DLT 재처리 시 claim 은 다시 1)
        assertThat(isPersisted(dropId, 2L)).isTrue();
    }

    @Test
    @DisplayName("[T4-34] 역직렬화 실패(JSON 아닌 바이트)는 재시도 없이 entry.created.DLT 로 가고 값은 원본 바이트 그대로다")
    void poisonPillGoesToDltWithOriginalBytes() throws Exception {
        // Given — JSON 이 아닌 바이트 (문서 3.0 "구현 시 확인": 원본 byte[] 가 그대로 DLT 에 기록되는가)
        long dropId = 3L;
        String key = "poison:" + UUID.randomUUID();
        byte[] poison = new byte[] {0x00, (byte) 0xFF, 0x7B, 0x6E, 0x6F, 0x74, 0x2D, 0x6A, 0x73, 0x6F, 0x6E};
        EntryCreated normal = event(dropId, 20L, 1);
        registerApplicant(dropId, normal);

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
            dlt.subscribe(List.of(Topics.ENTRY_CREATED_DLT));
            List<ConsumerRecord<String, byte[]>> received = new java.util.ArrayList<>();

            // When — poison pill 발행, 이어서 정상 이벤트 (파티션 1개라 순서대로 처리)
            long sentAt = System.nanoTime();
            rawProducer.send(new ProducerRecord<>(Topics.ENTRY_CREATED, key, poison)).get(5, TimeUnit.SECONDS);
            publish(normal);

            // Then — DLT 에 같은 key 의 레코드 1건
            await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(100)).until(() -> {
                dlt.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.stream().anyMatch(r -> key.equals(r.key()));
            });
            long elapsedMs = Duration.ofNanos(System.nanoTime() - sentAt).toMillis();
            List<ConsumerRecord<String, byte[]>> records =
                    received.stream().filter(r -> key.equals(r.key())).toList();
            assertThat(records).hasSize(1);

            // Then — 값이 원본 바이트 그대로
            assertThat(records.getFirst().value()).as("DLT 값이 원본 바이트와 같아야 한다").isEqualTo(poison);

            // Then — 재시도 없이 바로 (1초 간격 3회 재시도면 최소 3초)
            assertThat(elapsedMs).as("재시도(최소 3초) 없이 DLT 로 가야 한다").isLessThan(2_500L);
            Header exception = records.getFirst().headers().lastHeader("kafka_dlt-exception-fqcn");
            assertThat(exception).isNotNull();
            assertThat(new String(exception.value(), StandardCharsets.UTF_8)).contains("DeserializationException");

            // Then — 뒤이은 정상 이벤트는 저장된다 (poison pill 이 파티션을 막지 않음)
            await().atMost(WAIT).pollInterval(Duration.ofMillis(100))
                    .until(() -> countEntries(dropId, 20L) == 1);
        }
    }
}
