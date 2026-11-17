package com.queuedrop.ranking.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.event.UserRegistered;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.ranking.application.dto.DropRankResult;
import com.queuedrop.ranking.application.dto.UserRankResult;
import com.queuedrop.ranking.application.service.RankingQueryService;
import com.queuedrop.ranking.support.KafkaConsumption;
import com.queuedrop.ranking.support.KafkaTopicReadiness;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * 랭킹 리스너 3개(entry.created / drop.created / user.registered) 통합 테스트 (단계 문서 4장 T7-20~22).
 * presentation 테스트라 domain·infrastructure 타입을 쓰지 않는다 — 결과는 RankingQueryService(application Result)로 본다.
 * 조회 API(커밋 #5)가 아직 없어 단계 문서의 "/api/rankings/..." 대신 같은 결과를 내는 조회 서비스를 호출한다.
 * 토픽은 파티션 1개로 KafkaAdmin 이 만들고, 결과 확인 전에 앱 그룹(ranking-service)의 lag 0 을 기다린다.
 */
@SpringBootTest
@Import({RedisContainerConfig.class, KafkaContainerConfig.class, RankingListenerIntegrationTest.TopicConfig.class})
class RankingListenerIntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(15);
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final List<String> TOPICS = List.of(Topics.ENTRY_CREATED, Topics.DROP_CREATED, Topics.USER_REGISTERED);

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfig {
        /** 리스너 컨테이너 기동 전에 KafkaAdmin 이 만든다 — 자동 생성·메타데이터 갱신 지연에 기대지 않는다 */
        @Bean NewTopic entryCreatedTopic()   { return TopicBuilder.name(Topics.ENTRY_CREATED).partitions(1).replicas(1).build(); }
        @Bean NewTopic dropCreatedTopic()    { return TopicBuilder.name(Topics.DROP_CREATED).partitions(1).replicas(1).build(); }
        @Bean NewTopic userRegisteredTopic() { return TopicBuilder.name(Topics.USER_REGISTERED).partitions(1).replicas(1).build(); }
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired ConsumerFactory<?, ?> consumerFactory;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired StringRedisTemplate redis;
    @Autowired RankingQueryService queryService;

    /** dedupe·closeAt 이 실제 시각 기준 TTL 이라 기준은 실제 현재 시각 */
    private Instant now;

    @BeforeEach
    void setUp() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        KafkaTopicReadiness.awaitTopicMetadata(kafkaTemplate, TOPICS.toArray(String[]::new));
    }

    // ---- 발행 ----

    private void publishEntry(EntryCreated m) throws Exception {
        kafkaTemplate.send(Topics.ENTRY_CREATED, m.dropId() + ":" + m.userId(), m).get(5, TimeUnit.SECONDS);
    }

    private void publishDrop(DropCreated m) throws Exception {
        kafkaTemplate.send(Topics.DROP_CREATED, String.valueOf(m.dropId()), m).get(5, TimeUnit.SECONDS);
    }

    private void publishUser(UserRegistered m) throws Exception {
        kafkaTemplate.send(Topics.USER_REGISTERED, String.valueOf(m.userId()), m).get(5, TimeUnit.SECONDS);
    }

    private EntryCreated entry(long dropId, long userId) {
        return new EntryCreated(UUID.randomUUID().toString(), dropId, userId, 1, now);
    }

    /** 진행 중 드로우 (closeAt = 지금 + 1일) */
    private DropCreated drop(long dropId, String title) {
        return new DropCreated(dropId, 9L, title, 100, 10, now.minus(Duration.ofHours(1)), now.plus(Duration.ofDays(1)));
    }

    // ---- 대기 ----

    private void awaitConsumed(String... topics) {
        for (String topic : topics) {
            await("lag 0: " + topic).atMost(WAIT).pollInterval(Duration.ofMillis(100))
                    .until(() -> KafkaConsumption.fullyConsumed(kafkaAdmin, topic));
        }
    }

    private void awaitAllConsumed() {
        awaitConsumed(TOPICS.toArray(String[]::new));
    }

    /** appliedAt 의 Asia/Seoul ISO 주차 (week 인자를 명시해 테스트가 자정 경계에 흔들리지 않게 한다) */
    private static String weekOf(Instant at) {
        ZonedDateTime kst = at.atZone(SEOUL);
        return String.format("%d-W%02d", kst.get(IsoFields.WEEK_BASED_YEAR), kst.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
    }

    @Test
    @DisplayName("[T7-20 선행] 리스너 컨테이너 3개가 각 토픽을 group-id ranking-service 로 구독하고 ack-mode 는 RECORD, 자동 커밋은 꺼져 있다")
    void listenerContainersConfigured() {
        // When
        List<MessageListenerContainer> containers = List.copyOf(registry.getListenerContainers());
        Set<String> topics = containers.stream()
                .flatMap(c -> Set.of(c.getContainerProperties().getTopics()).stream())
                .collect(Collectors.toSet());

        // Then
        assertThat(containers).hasSize(3);
        assertThat(topics).containsExactlyInAnyOrderElementsOf(TOPICS);
        assertThat(containers).allSatisfy(c -> {
            assertThat(c.getGroupId()).isEqualTo(KafkaConsumption.APP_GROUP);
            assertThat(c.getContainerProperties().getAckMode()).isEqualTo(AckMode.RECORD);
        });
        assertThat(String.valueOf(consumerFactory.getConfigurationProperties()
                .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))).isEqualTo("false");
    }

    @Test
    @DisplayName("[T7-20] user.registered·drop.created·entry.created 를 소비하면 드로우 제목·닉네임이 랭킹에 붙고, 표시 정보가 없는 id 는 drop-{id}·user-{id} 로 나온다")
    void listenersAggregateAndAttachDisplayNames() throws Exception {
        // Given — 3001/2001 은 표시 정보 있음, 3002/2002 는 없음
        publishUser(new UserRegistered(2001L, "원영"));
        publishDrop(drop(3001L, "한정판 스니커즈"));

        // When
        publishEntry(entry(3001L, 2001L));
        publishEntry(entry(3002L, 2002L));

        // Then — 세 토픽 모두 커밋된 뒤
        awaitAllConsumed();
        List<DropRankResult> drops = queryService.topDrops(10);
        assertThat(drops).extracting(DropRankResult::dropId, DropRankResult::title, DropRankResult::entryCount)
                .containsExactlyInAnyOrder(tuple(3001L, "한정판 스니커즈", 1L), tuple(3002L, "drop-3002", 1L));
        assertThat(drops).extracting(DropRankResult::rank).containsExactly(1L, 2L);

        List<UserRankResult> users = queryService.allUsers(10);
        assertThat(users).extracting(UserRankResult::userId, UserRankResult::nickname, UserRankResult::score)
                .containsExactlyInAnyOrder(tuple(2001L, "원영", 1L), tuple(2002L, "user-2002", 1L));

        assertThat(queryService.weeklyUsers(weekOf(now), 10).items())
                .extracting(UserRankResult::userId, UserRankResult::nickname, UserRankResult::score)
                .containsExactlyInAnyOrder(tuple(2001L, "원영", 1L), tuple(2002L, "user-2002", 1L));
    }

    @Test
    @DisplayName("[T7-21] 같은 entry.created 메시지를 3번 받아도 3건 모두 커밋(lag 0)된 뒤 인기·주간·누적 점수는 모두 1이다 (dedupe)")
    void redeliveredEntryCountedOnce() throws Exception {
        // Given — eventId 까지 같은 메시지
        EntryCreated message = entry(3101L, 2101L);

        // When
        publishEntry(message);
        publishEntry(message);
        publishEntry(message);

        // Then — 점수가 아니라 커밋을 기다린다 (점수만 보면 1번째 처리 직후 통과)
        awaitConsumed(Topics.ENTRY_CREATED);
        assertThat(queryService.topDrops(10)).extracting(DropRankResult::dropId, DropRankResult::entryCount)
                .containsExactly(tuple(3101L, 1L));
        assertThat(queryService.allUsers(10)).extracting(UserRankResult::userId, UserRankResult::score)
                .containsExactly(tuple(2101L, 1L));
        assertThat(queryService.weeklyUsers(weekOf(now), 10).items())
                .extracting(UserRankResult::userId, UserRankResult::score)
                .containsExactly(tuple(2101L, 1L));
    }

    @Test
    @DisplayName("[T7-22] entry.created 가 drop.created·user.registered 보다 먼저 와도 집계되고 제목은 drop-{id}, 이후 표시 정보가 오면 채워지며 점수는 그대로다")
    void entryBeforeDisplayInfo() throws Exception {
        // Given — 표시 정보 없이 응모만 먼저
        publishEntry(entry(3201L, 2201L));
        awaitConsumed(Topics.ENTRY_CREATED);
        assertThat(queryService.topDrops(10)).extracting(DropRankResult::dropId, DropRankResult::title, DropRankResult::entryCount)
                .containsExactly(tuple(3201L, "drop-3201", 1L));
        assertThat(queryService.allUsers(10)).extracting(UserRankResult::userId, UserRankResult::nickname)
                .containsExactly(tuple(2201L, "user-2201"));

        // When — 늦게 도착한 표시 정보
        publishDrop(drop(3201L, "늦게 온 제목"));
        publishUser(new UserRegistered(2201L, "늦은닉네임"));

        // Then
        awaitAllConsumed();
        assertThat(queryService.topDrops(10)).extracting(DropRankResult::dropId, DropRankResult::title, DropRankResult::entryCount)
                .containsExactly(tuple(3201L, "늦게 온 제목", 1L));
        assertThat(queryService.allUsers(10)).extracting(UserRankResult::userId, UserRankResult::nickname, UserRankResult::score)
                .containsExactly(tuple(2201L, "늦은닉네임", 1L));
    }
}
