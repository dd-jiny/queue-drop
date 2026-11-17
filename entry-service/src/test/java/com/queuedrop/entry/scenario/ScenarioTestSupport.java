package com.queuedrop.entry.scenario;

import com.queuedrop.common.event.Topics;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import com.queuedrop.entry.infrastructure.messaging.KafkaEntryEventPublisher;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 4단계 시나리오 테스트 공통 지원 (단계 문서 4.3, 4.4).
 * 세 시나리오 클래스가 같은 @Import 조합을 써서 컨텍스트(와 컨테이너)를 공유한다.
 */
final class ScenarioTestSupport {

    /** 고정 판정 시각. 드로우는 OPEN_AT ~ CLOSE_AT 로 등록한다 */
    static final Instant NOW = Instant.parse("2026-11-20T10:30:00Z");
    static final Instant OPEN_AT = Instant.parse("2026-11-20T10:00:00Z");
    static final Instant CLOSE_AT = Instant.parse("2026-11-20T12:00:00Z");
    static final String CONSUMER_GROUP = "entry-service";

    private ScenarioTestSupport() {}

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {
        /** 운영과 같은 파티션 수 (entry.created 6). 같은 key 는 같은 파티션이라 사용자별 순서는 유지된다 */
        @Bean NewTopic dropCreatedTopic()     { return TopicBuilder.name(Topics.DROP_CREATED).partitions(1).replicas(1).build(); }
        @Bean NewTopic entryCreatedTopic()    { return TopicBuilder.name(Topics.ENTRY_CREATED).partitions(6).replicas(1).build(); }
        @Bean NewTopic dropCreatedDltTopic()  { return TopicBuilder.name(Topics.DROP_CREATED_DLT).partitions(1).replicas(1).build(); }
        @Bean NewTopic entryCreatedDltTopic() { return TopicBuilder.name(Topics.ENTRY_CREATED_DLT).partitions(6).replicas(1).build(); }

        @Bean @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean @Primary
        ControllableEntryEventPublisher controllableEntryEventPublisher(KafkaEntryEventPublisher delegate,
                                                                        JdbcTemplate jdbc) {
            return new ControllableEntryEventPublisher(delegate, jdbc);
        }
    }

    /**
     * 테스트 전용 Publisher (단계 문서 4.4). 기본은 실제 KafkaEntryEventPublisher 로 위임한다.
     * - failOnceFor: 보내지 않고 실패 (발행 실패, T4-30)
     * - loseAckOnceFor: 실제로 보낸 뒤 실패 (ack 유실 → 유령 이벤트, T4-31 순서 A)
     * - consumeThenFailOnceFor: 실제로 보내고 저장 컨슈머가 DB 에 저장할 때까지 기다린 뒤 실패
     *   (= 단계 문서 4.4 ConsumedThenFailPublisher, 소비가 보상보다 먼저, T4-42 순서 B).
     *   EntryJpaRepository 가 package-private 이라 저장 여부는 JdbcTemplate 로 확인한다.
     *   별도 @Primary 빈 대신 이 클래스에 모드로 둬 시나리오 클래스들이 같은 컨텍스트를 공유한다.
     */
    static final class ControllableEntryEventPublisher implements EntryEventPublisher {

        private final EntryEventPublisher delegate;
        private final Set<Long> failOnce = ConcurrentHashMap.newKeySet();
        private final Set<Long> ackLostOnce = ConcurrentHashMap.newKeySet();
        private final Set<Long> consumedThenFailOnce = ConcurrentHashMap.newKeySet();
        private final JdbcTemplate jdbc;

        ControllableEntryEventPublisher(EntryEventPublisher delegate, JdbcTemplate jdbc) {
            this.delegate = delegate;
            this.jdbc = jdbc;
        }

        void failOnceFor(long userId)     { failOnce.add(userId); }
        void loseAckOnceFor(long userId)  { ackLostOnce.add(userId); }
        void consumeThenFailOnceFor(long userId) { consumedThenFailOnce.add(userId); }
        void reset() {
            failOnce.clear();
            ackLostOnce.clear();
            consumedThenFailOnce.clear();
        }

        @Override
        public void publishCreated(EntryCreatedPayload payload) {
            if (failOnce.remove(payload.userId())) {
                throw new EventPublishException("발행 실패 시뮬레이션", null);
            }
            delegate.publishCreated(payload);                     // 브로커에는 기록됨
            if (ackLostOnce.remove(payload.userId())) {
                throw new EventPublishException("ack 유실 시뮬레이션", null);
            }
            if (consumedThenFailOnce.remove(payload.userId())) {
                // claim 은 INSERT 전에 실행되므로 "DB 에 행이 있음" 이면 claim 이 보상보다 먼저임이 보장된다
                await().atMost(ScenarioTestBase.CONSUME_WAIT).pollInterval(Duration.ofMillis(50)).until(() ->
                        persisted(payload.dropId(), payload.userId()));
                throw new EventPublishException("ack 유실 시뮬레이션 (컨슈머가 먼저 처리)", null);
            }
        }

        private boolean persisted(long dropId, long userId) {
            Long count = jdbc.queryForObject(
                    "SELECT count(*) FROM entries WHERE drop_id = ? AND user_id = ?", Long.class, dropId, userId);
            return count != null && count > 0;
        }
    }

    /**
     * 앱 컨슈머 그룹이 entry.created 를 끝까지 처리(커밋)했는가. ack-mode=record 라 레코드 처리 후 커밋된다.
     * "DB 건수가 N 이 될 때까지" 만 기다리면 늦게 온 이벤트(유령 등)를 놓칠 수 있어 lag 0 을 함께 본다.
     */
    static boolean entryCreatedFullyConsumed(KafkaAdmin kafkaAdmin) throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<TopicPartition> partitions = admin.describeTopics(Set.of(Topics.ENTRY_CREATED))
                    .allTopicNames().get().get(Topics.ENTRY_CREATED).partitions().stream()
                    .map(p -> new TopicPartition(Topics.ENTRY_CREATED, p.partition()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(
                    partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                    .all().get();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata().get();
            return partitions.stream().allMatch(tp -> {
                long end = ends.get(tp).offset();
                OffsetAndMetadata c = committed.get(tp);
                return end == 0 || (c != null && c.offset() >= end);
            });
        }
    }
}
