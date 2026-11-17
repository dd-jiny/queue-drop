package com.queuedrop.ranking.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;

import ch.qos.logback.classic.Level;
import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.ranking.application.dto.DropRankResult;
import com.queuedrop.ranking.application.dto.RankingRecordCommand;
import com.queuedrop.ranking.application.service.RankingAggregateService;
import com.queuedrop.ranking.application.service.RankingQueryService;
import com.queuedrop.ranking.support.CapturedLogs;
import com.queuedrop.ranking.support.KafkaConsumption;
import com.queuedrop.ranking.support.KafkaTopicReadiness;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;

/**
 * 리스너 실패 정책 (단계 문서 3.3, T7-25·T7-26): 1초 간격 3회 재시도 후 ERROR 로그(topic·partition·offset·key)를 남기고
 * 건너뛴다 (DLT 없음). 역직렬화 실패는 재시도 없이 바로 건너뛴다.
 *
 * 장애 주입: presentation 테스트가 domain 타입(RankingRepository·RankingStoreUnavailableException)을 쓰지 않도록
 * 저장소를 Mock 으로 바꾸지 않고, 실패시킬 드로우의 dedupe 키를 String 으로 만들어 rank.lua 의 SISMEMBER 가
 * WRONGTYPE 으로 실패하게 한다 → 실제 경로(Redis 예외 → RankingStoreUnavailableException → 리스너 밖으로 전파)를 탄다.
 * 호출 횟수·시각은 RankingAggregateService 스파이(실제 메서드 호출)로 기록한다.
 */
@SpringBootTest
@Import({RedisContainerConfig.class, KafkaContainerConfig.class, RankingListenerIntegrationTest.TopicConfig.class})
class RankingListenerRetryIntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(25);
    private static final long FAILING_DROP = 4001L;
    private static final long NEXT_DROP = 4002L;

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired KafkaContainer kafka;
    @Autowired StringRedisTemplate redis;
    @Autowired RankingQueryService queryService;
    @Autowired KafkaListenerEndpointRegistry registry;
    @MockitoSpyBean RankingAggregateService aggregateService;

    private final Map<Long, List<Instant>> calls = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        calls.clear();
        willAnswer(invocation -> {
            RankingRecordCommand cmd = invocation.getArgument(0);
            calls.computeIfAbsent(cmd.dropId(), id -> new CopyOnWriteArrayList<>()).add(Instant.now());
            return invocation.callRealMethod();
        }).given(aggregateService).recordEntry(any());
        KafkaTopicReadiness.awaitTopicMetadata(kafkaTemplate, Topics.ENTRY_CREATED);
        awaitEntryCreatedAssigned();
    }

    /** 경과 시간 단언(T7-26)이 첫 그룹 참여(브로커 initial rebalance 지연)에 흔들리지 않도록 파티션 배정을 먼저 기다린다 */
    private void awaitEntryCreatedAssigned() {
        await("entry.created 파티션 배정").atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> registry.getListenerContainers().stream()
                        .filter(c -> List.of(c.getContainerProperties().getTopics() == null
                                ? new String[0] : c.getContainerProperties().getTopics()).contains(Topics.ENTRY_CREATED))
                        .anyMatch(c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty()));
    }

    private List<Instant> callsOf(long dropId) {
        return calls.getOrDefault(dropId, List.of());
    }

    private static String keyOf(EntryCreated m) {
        return m.dropId() + ":" + m.userId();
    }

    private RecordMetadata publish(EntryCreated m) throws Exception {
        return kafkaTemplate.send(Topics.ENTRY_CREATED, keyOf(m), m).get(5, TimeUnit.SECONDS).getRecordMetadata();
    }

    private static EntryCreated entry(long dropId, long userId) {
        return new EntryCreated(UUID.randomUUID().toString(), dropId, userId, 1, Instant.now());
    }

    private void awaitEntryCreatedConsumed() {
        await("lag 0: entry.created").atMost(WAIT).pollInterval(Duration.ofMillis(100))
                .until(() -> KafkaConsumption.fullyConsumed(kafkaAdmin, Topics.ENTRY_CREATED));
    }

    private Set<String> topicNames() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return admin.listTopics().names().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("[T7-25] Redis 장애가 계속되면 1초 간격 3회 재시도(총 4회) 후 ERROR 로그(topic·partition·offset·key)를 남기고 건너뛰며, 다음 메시지는 1회 처리된다 (DLT 없음)")
    void retriesThreeTimesThenSkips() throws Exception {
        // Given — FAILING_DROP 의 dedupe 키가 Set 이 아니라 String → rank.lua 가 항상 WRONGTYPE
        redis.opsForValue().set("rank:dedupe:" + FAILING_DROP, "broken");
        EntryCreated failing = entry(FAILING_DROP, 51L);
        EntryCreated next = entry(NEXT_DROP, 52L);

        try (CapturedLogs logs = new CapturedLogs("com.queuedrop.ranking")) {
            // When — 파티션 1개라 NEXT 는 FAILING 의 재시도가 끝난 뒤 처리된다
            RecordMetadata sent = publish(failing);
            publish(next);

            // Then — 둘 다 커밋될 때까지
            awaitEntryCreatedConsumed();
            await().atMost(WAIT).pollInterval(Duration.ofMillis(100)).until(() -> !callsOf(NEXT_DROP).isEmpty());

            List<Instant> failingCalls = callsOf(FAILING_DROP);
            assertThat(failingCalls).as("첫 호출 + 재시도 3회").hasSize(4);
            assertThat(Duration.between(failingCalls.getFirst(), failingCalls.getLast()))
                    .as("1초 간격 재시도 (지터 여유 0.5초)")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(2_500));
            assertThat(callsOf(NEXT_DROP)).hasSize(1);
            assertThat(queryService.topDrops(10)).extracting(DropRankResult::dropId, DropRankResult::entryCount)
                    .containsExactly(tuple(NEXT_DROP, 1L));

            // Then — recoverer ERROR 로그는 레코드당 1줄, 필드 이름까지 단언
            String key = keyOf(failing);
            await("recoverer ERROR 로그").atMost(WAIT).pollInterval(Duration.ofMillis(100))
                    .until(() -> logs.messages(Level.ERROR).stream().anyMatch(m -> m.contains("key=" + key)));
            List<String> errorLogs = logs.messages(Level.ERROR).stream().filter(m -> m.contains("key=" + key)).toList();
            assertThat(errorLogs).hasSize(1);
            assertThat(errorLogs.getFirst())
                    .contains("topic=" + Topics.ENTRY_CREATED + " ")
                    .contains("partition=" + sent.partition() + " ")
                    .contains("offset=" + sent.offset() + " ")
                    .endsWith("key=" + key);
        }

        // Then — DLT 로 보내지 않는다 (DLT 토픽이 생기지 않음)
        assertThat(topicNames()).noneMatch(name -> name.toUpperCase().contains("DLT"));
    }

    @Test
    @DisplayName("[T7-26] JSON 이 아닌 entry.created 레코드는 재시도 없이 ERROR 로그 후 건너뛰고, 뒤이은 정상 메시지가 집계된다 (파티션이 막히지 않음)")
    void poisonPillSkippedWithoutRetry() throws Exception {
        // Given — 별도 bytes 프로듀서로 보낼 깨진 값, 뒤이을 정상 메시지
        String poisonKey = "poison-" + UUID.randomUUID();
        byte[] poison = new byte[] {0x00, (byte) 0xFF, 0x7B, 0x6E, 0x6F, 0x74, 0x2D, 0x6A, 0x73, 0x6F, 0x6E};
        EntryCreated normal = entry(4101L, 61L);

        try (CapturedLogs logs = new CapturedLogs("com.queuedrop.ranking");
             var rawProducer = new KafkaProducer<String, byte[]>(Map.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {

            // When — poison pill, 이어서 정상 메시지 (파티션 1개라 순서대로 처리)
            long sentAt = System.nanoTime();
            RecordMetadata sent = rawProducer.send(new ProducerRecord<>(Topics.ENTRY_CREATED, poisonKey, poison))
                    .get(5, TimeUnit.SECONDS);
            publish(normal);

            // Then — 정상 메시지 집계 + 두 건 모두 커밋
            awaitEntryCreatedConsumed();
            await().atMost(WAIT).pollInterval(Duration.ofMillis(100)).until(() -> !callsOf(4101L).isEmpty());
            long elapsedMs = Duration.ofNanos(System.nanoTime() - sentAt).toMillis();
            assertThat(queryService.topDrops(10)).extracting(DropRankResult::dropId, DropRankResult::entryCount)
                    .containsExactly(tuple(4101L, 1L));
            assertThat(callsOf(4101L)).hasSize(1);
            assertThat(calls.keySet()).as("깨진 레코드는 서비스까지 오지 않는다").containsExactly(4101L);

            // Then — 재시도(1초 간격 3회면 최소 3초) 없이 건너뛰었다
            assertThat(elapsedMs).as("재시도 없이 건너뛰어야 한다").isLessThan(2_500L);

            // Then — recoverer ERROR 로그 1줄 (topic·partition·offset·key)
            await("recoverer ERROR 로그").atMost(WAIT).pollInterval(Duration.ofMillis(100))
                    .until(() -> logs.messages(Level.ERROR).stream().anyMatch(m -> m.contains("key=" + poisonKey)));
            List<String> errorLogs = logs.messages(Level.ERROR).stream().filter(m -> m.contains("key=" + poisonKey)).toList();
            assertThat(errorLogs).hasSize(1);
            assertThat(errorLogs.getFirst())
                    .contains("topic=" + Topics.ENTRY_CREATED + " ")
                    .contains("partition=" + sent.partition() + " ")
                    .contains("offset=" + sent.offset() + " ");
        }
    }
}
