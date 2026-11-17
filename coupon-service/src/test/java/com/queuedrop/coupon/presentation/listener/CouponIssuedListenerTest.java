package com.queuedrop.coupon.presentation.listener;

import static com.queuedrop.coupon.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.coupon.support.CapturedLogs;
import com.queuedrop.coupon.support.KafkaConsumption;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * coupon.issued 저장 리스너 통합 테스트 (단계 문서 4장 T6-17, T6-23, T6-29).
 * presentation 테스트라 domain·infrastructure 타입을 쓰지 않는다 — DB 는 JdbcTemplate, Redis 는 키 문자열로 직접 본다.
 * 발급 상태는 coupon-issue.lua 의 OK 결과와 같은 상태(SADD issued + HSET requests)를 키로 직접 만든다.
 * 저장 실패(T6-23)는 특정 user_id INSERT 를 거부하는 DB 트리거로 주입한다 (저장소 포트가 domain 타입이라 대체 빈을 두지 않는다).
 * 토픽은 파티션 1개 — 나중에 보낸 메시지가 처리됐으면 앞선 메시지도 처리됐다.
 */
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        CouponIssuedListenerTest.TopicConfig.class})
class CouponIssuedListenerTest {

    private static final Duration WAIT = Duration.ofSeconds(15);
    /** 1초 간격 3회 재시도(약 3초) + DLT 발행 + 수신 여유 */
    private static final Duration DLT_WAIT = Duration.ofSeconds(25);
    private static final Instant ISSUED_AT = Instant.parse("2026-11-20T10:30:00.123456Z");
    private static final long FAILING_USER = 2_300L;

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        @Bean NewTopic couponIssuedTopic()    { return TopicBuilder.name(Topics.COUPON_ISSUED).partitions(1).replicas(1).build(); }
        @Bean NewTopic couponIssuedDltTopic() { return TopicBuilder.name(Topics.COUPON_ISSUED_DLT).partitions(1).replicas(1).build(); }
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired StringRedisTemplate redis;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired KafkaContainer kafka;
    @Autowired KafkaListenerEndpointRegistry registry;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE issued_coupons, coupon_events RESTART IDENTITY");
        // NewTopic 직후 첫 send 가 max.block.ms(1초) 안에 메타데이터를 못 받는 것을 막는다 (원본·DLT)
        awaitTopicMetadata(kafkaTemplate, Topics.COUPON_ISSUED, Topics.COUPON_ISSUED_DLT);
    }

    @AfterEach
    void dropFailureTrigger() {
        jdbc.execute("DROP TRIGGER IF EXISTS t6_23_fail_insert ON issued_coupons");
        jdbc.execute("DROP FUNCTION IF EXISTS t6_23_fail_insert()");
    }

    private long givenCouponEvent() {
        return jdbc.queryForObject("""
                INSERT INTO coupon_events (name, total_quantity, issue_start_at, created_at)
                VALUES ('리스너 쿠폰', 100, ?, ?) RETURNING id""", Long.class,
                Timestamp.from(ISSUED_AT.minusSeconds(3600)), Timestamp.from(ISSUED_AT.minusSeconds(7200)));
    }

    /** coupon-issue.lua 가 OK 일 때 남기는 상태: issued 에 userId, requests[userId] = 요청 ID (claim true 조건) */
    private void givenIssuedInRedis(CouponIssued message) {
        redis.opsForSet().add("coupon:" + message.couponEventId() + ":issued", String.valueOf(message.userId()));
        redis.opsForHash().put("coupon:" + message.couponEventId() + ":requests",
                String.valueOf(message.userId()), message.eventId());
    }

    private CouponIssued message(long couponEventId, long userId) {
        return new CouponIssued(UUID.randomUUID().toString(), couponEventId, userId, ISSUED_AT);
    }

    private static String keyOf(CouponIssued message) {
        return message.couponEventId() + ":" + message.userId();
    }

    private RecordMetadata publish(CouponIssued message) throws Exception {
        return kafkaTemplate.send(Topics.COUPON_ISSUED, keyOf(message), message).get(5, TimeUnit.SECONDS)
                .getRecordMetadata();
    }

    private long dbCount(long couponEventId, long userId) {
        return jdbc.queryForObject("SELECT count(*) FROM issued_coupons WHERE coupon_event_id = ? AND user_id = ?",
                Long.class, couponEventId, userId);
    }

    private KafkaConsumer<String, byte[]> dltBytesConsumer() {
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
        consumer.subscribe(List.of(Topics.COUPON_ISSUED_DLT));
        return consumer;
    }

    @Test
    @DisplayName("[T6-16 선행] 저장 리스너 컨테이너는 id=coupon-saver 로 등록되고 group-id 는 coupon-service 그대로다 (idIsGroup=false, pause 용)")
    void saverContainerIdAndGroup() {
        // When
        MessageListenerContainer container = registry.getListenerContainer("coupon-saver");

        // Then
        assertThat(container).as("@KafkaListener(id = \"coupon-saver\", idIsGroup = false)").isNotNull();
        assertThat(container.getGroupId()).isEqualTo(KafkaConsumption.APP_GROUP);
    }

    @Test
    @DisplayName("[T6-17] 같은 coupon.issued 메시지를 3번 받아도 DB 에는 1건만 남고 필드가 메시지와 같다 (재전달 멱등)")
    void redeliveryKeepsSingleRow() throws Exception {
        // Given — Redis 발급 상태(issued + requests = 메시지 eventId)
        long eventId = givenCouponEvent();
        CouponIssued message = message(eventId, 10L);
        givenIssuedInRedis(message);

        // When — 같은 메시지(eventId 포함 동일) 3번
        publish(message);
        publish(message);
        publish(message);

        // Then — 세 건 모두 처리(커밋)된 뒤 1건
        await().atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> KafkaConsumption.fullyConsumed(kafkaAdmin, Topics.COUPON_ISSUED));
        assertThat(dbCount(eventId, 10L)).isEqualTo(1);
        Timestamp issuedAt = jdbc.queryForObject(
                "SELECT issued_at FROM issued_coupons WHERE coupon_event_id = ? AND user_id = ?",
                Timestamp.class, eventId, 10L);
        assertThat(issuedAt.toInstant()).isEqualTo(ISSUED_AT);
        assertThat(redis.opsForSet().isMember("coupon:" + eventId + ":persisted", "10")).isTrue();
    }

    @Test
    @DisplayName("[T6-23] 저장이 계속 실패하면 3회 재시도 후 coupon.issued.DLT 에 1건, DLT 적재 ERROR 로그 1줄(topic·offset·key), DB 0건")
    void saveFailureGoesToDltWithErrorLog() throws Exception {
        // Given — claim true 상태, 이 사용자 INSERT 는 DB 가 거부 (저장소가 항상 예외)
        long eventId = givenCouponEvent();
        CouponIssued message = message(eventId, FAILING_USER);
        givenIssuedInRedis(message);
        jdbc.execute("""
                CREATE OR REPLACE FUNCTION t6_23_fail_insert() RETURNS trigger AS $$
                BEGIN RAISE EXCEPTION 'T6-23 저장 실패 주입'; END; $$ LANGUAGE plpgsql""");
        jdbc.execute("CREATE TRIGGER t6_23_fail_insert BEFORE INSERT ON issued_coupons FOR EACH ROW "
                + "WHEN (NEW.user_id = " + FAILING_USER + ") EXECUTE FUNCTION t6_23_fail_insert()");
        String key = keyOf(message);

        try (CapturedLogs logs = new CapturedLogs("com.queuedrop.coupon");
             KafkaConsumer<String, byte[]> dlt = dltBytesConsumer()) {
            List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();

            // When
            long sentAt = System.nanoTime();
            RecordMetadata sent = publish(message);

            // Then — 재시도 후 DLT 에 같은 key 1건
            await().atMost(DLT_WAIT).pollInterval(Duration.ofMillis(100)).until(() -> {
                dlt.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.stream().anyMatch(r -> key.equals(r.key()));
            });
            long elapsedMs = Duration.ofNanos(System.nanoTime() - sentAt).toMillis();
            List<ConsumerRecord<String, byte[]>> records = received.stream().filter(r -> key.equals(r.key())).toList();
            assertThat(records).hasSize(1);
            assertThat(elapsedMs).as("1초 간격 3회 재시도 후 DLT").isGreaterThanOrEqualTo(2_500L);
            Header originalTopic = records.getFirst().headers().lastHeader("kafka_dlt-original-topic");
            assertThat(originalTopic).isNotNull();
            assertThat(new String(originalTopic.value(), StandardCharsets.UTF_8)).isEqualTo(Topics.COUPON_ISSUED);
            assertThat(new String(records.getFirst().value(), StandardCharsets.UTF_8))
                    .as("DLT 값은 JSON 메시지").contains(message.eventId());

            // Then — DLT 적재 ERROR 로그 1줄 (Redis 에만 남은 발급을 감지할 유일한 수단, v1.6)
            await("DLT 적재 ERROR 로그 (key 포함)").atMost(WAIT).pollInterval(Duration.ofMillis(100))
                    .until(() -> logs.messages(Level.ERROR).stream().anyMatch(m -> m.contains(key)));
            List<String> dltLogs = logs.messages(Level.ERROR).stream().filter(m -> m.contains(key)).toList();
            assertThat(dltLogs).as("DLT 적재 ERROR 로그는 레코드당 1줄").hasSize(1);
            // 고정 문구에도 "coupon.issued" 가 있고 offset 은 한 자리 수라, 필드 이름까지 넣어 단언한다
            // (형식: coupon.issued DLT 적재 topic={} partition={} offset={} key={})
            assertThat(dltLogs.getFirst())
                    .contains("topic=" + Topics.COUPON_ISSUED + " ")
                    .contains("partition=" + sent.partition() + " ")
                    .contains("offset=" + sent.offset() + " ")
                    .endsWith("key=" + key);
        }
        assertThat(dbCount(eventId, FAILING_USER)).isZero();
    }

    @Test
    @DisplayName("[T6-29] 역직렬화 실패(JSON 아닌 바이트)는 재시도 없이 coupon.issued.DLT 로 원본 바이트 그대로 가고, 다음 메시지는 정상 저장된다")
    void poisonPillGoesToDltWithOriginalBytes() throws Exception {
        // Given — JSON 이 아닌 바이트, 뒤이을 정상 메시지
        long eventId = givenCouponEvent();
        String key = "poison:" + UUID.randomUUID();
        byte[] poison = new byte[] {0x00, (byte) 0xFF, 0x7B, 0x6E, 0x6F, 0x74, 0x2D, 0x6A, 0x73, 0x6F, 0x6E};
        CouponIssued normal = message(eventId, 20L);
        givenIssuedInRedis(normal);

        try (KafkaConsumer<String, byte[]> dlt = dltBytesConsumer();
             var rawProducer = new KafkaProducer<String, byte[]>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();

            // When — poison pill, 이어서 정상 메시지 (파티션 1개라 순서대로 처리)
            long sentAt = System.nanoTime();
            rawProducer.send(new ProducerRecord<>(Topics.COUPON_ISSUED, key, poison)).get(5, TimeUnit.SECONDS);
            publish(normal);

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

            // Then — 뒤이은 정상 메시지는 저장된다 (poison pill 이 파티션을 막지 않음)
            await().atMost(WAIT).pollInterval(Duration.ofMillis(100)).until(() -> dbCount(eventId, 20L) == 1);
        }
    }
}
