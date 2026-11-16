package com.queuedrop.product.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.product.application.dto.DropRegisterCommand;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.port.DropEventPublisher;
import com.queuedrop.product.application.service.DropService;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@Import({PostgresContainerConfig.class, KafkaContainerConfig.class})
class DropCreatedPublishIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration QUIET_PERIOD = Duration.ofSeconds(3);
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(15);
    private static final Requester ADMIN = new Requester(1L, "ADMIN");

    @Autowired DropService dropService;
    @Autowired DropEventPublisher eventPublisher;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired KafkaContainer kafka;

    private KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() throws Exception {
        createTopicIfAbsent();
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "drop-created-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(Topics.DROP_CREATED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    /** auto-create 에 기대지 않고 구독 전에 토픽을 만들어 둔다 (첫 메시지 유실·리밸런스 지연 방지). */
    private void createTopicIfAbsent() throws InterruptedException {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(Topics.DROP_CREATED, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** 지금까지 받은 메시지 중 key 가 일치하는 것. 호출할 때마다 한 번 poll 해 누적한다. */
    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> key.equals(r.key())).toList();
    }

    private Long givenProductId() {
        return jdbcTemplate.queryForObject(
                "insert into products (name, created_at) values ('통합테스트 상품', now()) returning id", Long.class);
    }

    /** 시스템 Clock 기준이므로 오픈 1분 여유를 넉넉히 넘기도록 now+1시간 이후로 잡는다. DB 정밀도(µs)에 맞춘다. */
    private static Instant openAt() {
        return Instant.now().plus(Duration.ofHours(1)).truncatedTo(ChronoUnit.MICROS);
    }

    private DropRegisterCommand command(Long productId, String title, Instant openAt) {
        return new DropRegisterCommand(ADMIN, productId, title, 100, 10, openAt, openAt.plus(Duration.ofHours(2)));
    }

    private int countDrops(String title) {
        return jdbcTemplate.queryForObject("select count(*) from drops where title = ?", Integer.class, title);
    }

    @Test
    @DisplayName("[T2-21] 발행 포트 빈은 KafkaDropEventPublisher 다")
    void publisherBeanIsKafkaAdapter() {
        // Then
        assertThat(AopUtils.getTargetClass(eventPublisher)).isEqualTo(KafkaDropEventPublisher.class);
    }

    @Test
    @DisplayName("[T2-21] ADMIN 이 드로우를 등록하고 커밋되면 drop.created 에 key=dropId, 필드 일치 메시지 1건이 수신된다")
    void registerPublishesAfterCommit() {
        // Given
        Long productId = givenProductId();
        String title = "커밋-" + UUID.randomUUID();
        Instant openAt = openAt();
        Instant closeAt = openAt.plus(Duration.ofHours(2));

        // When: DropService.register 의 @Transactional 이 커밋된다
        DropResult result = dropService.register(command(productId, title, openAt));

        // Then: DB
        assertThat(countDrops(title)).isEqualTo(1);

        // Then: 메시지
        String key = String.valueOf(result.id());
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> recordsWithKey(key).size() == 1);
        DropCreated message = JSON.readValue(recordsWithKey(key).getFirst().value(), DropCreated.class);
        assertThat(message).isEqualTo(new DropCreated(result.id(), productId, title, 100, 10, openAt, closeAt));
    }

    @Test
    @DisplayName("[T2-21] 트랜잭션 안에서 등록한 직후(커밋 전)에는 미발행이고, 커밋 후에 1건 수신된다")
    void notPublishedBeforeCommit() {
        // Given
        Long productId = givenProductId();
        String title = "커밋전-" + UUID.randomUUID();

        // When: 바깥 트랜잭션 안에서 register (REQUIRED 로 합류) 후 커밋 전 시점에 소비자를 확인
        String key = transactionTemplate.execute(tx -> {
            DropResult result = dropService.register(command(productId, title, openAt()));
            String dropKey = String.valueOf(result.id());

            // Then: 커밋 전에는 대기 시간 동안 도착하지 않는다
            await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(2))
                    .until(() -> recordsWithKey(dropKey).isEmpty());
            return dropKey;
        });

        // Then: 커밋 후에는 1건 도착한다
        await().atMost(RECEIVE_TIMEOUT).until(() -> recordsWithKey(key).size() == 1);
    }

    @Test
    @DisplayName("[T2-22] 같은 트랜잭션에서 등록 후 예외로 롤백되면 3초 동안 drop.created 미수신, DB 0건 (커밋된 대조군은 수신)")
    void rollbackDoesNotPublish() {
        // Given
        Long productId = givenProductId();
        String rolledBackTitle = "롤백-" + UUID.randomUUID();
        String committedTitle = "대조군-" + UUID.randomUUID();
        List<Long> rolledBackIds = new ArrayList<>();

        // When: 등록 후 예외로 롤백
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(tx -> {
            rolledBackIds.add(dropService.register(command(productId, rolledBackTitle, openAt())).id());
            throw new IllegalStateException("force rollback");
        })).isInstanceOf(IllegalStateException.class);

        // When: 대조군 — 같은 방식으로 커밋
        Long committedId = transactionTemplate.execute(tx ->
                dropService.register(command(productId, committedTitle, openAt())).id());

        // Then: DB
        assertThat(countDrops(rolledBackTitle)).isZero();
        assertThat(countDrops(committedTitle)).isEqualTo(1);

        // Then: 커밋된 쪽은 도착한다 (브로커·컨슈머가 정상임을 확인)
        await().atMost(RECEIVE_TIMEOUT).until(() -> recordsWithKey(String.valueOf(committedId)).size() == 1);

        // Then: 롤백된 쪽은 대기 시간 동안 끝내 도착하지 않는다
        String rolledBackKey = String.valueOf(rolledBackIds.getFirst());
        await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(2))
                .until(() -> recordsWithKey(rolledBackKey).isEmpty());
    }

    @Test
    @DisplayName("[T2-23] 트랜잭션 밖 republish 는 즉시 drop.created 1건을 발행한다 (저장 값 그대로)")
    void republishOutsideTransactionPublishesImmediately() {
        // Given: 발행 없이 DB 에 직접 둔 드로우 (이미 오픈된 드로우도 재발행 가능)
        Long productId = givenProductId();
        String title = "재발행-" + UUID.randomUUID();
        Instant openAt = Instant.now().minus(Duration.ofMinutes(10)).truncatedTo(ChronoUnit.MICROS);
        Instant closeAt = openAt.plus(Duration.ofHours(2));
        Long dropId = jdbcTemplate.queryForObject(
                "insert into drops (product_id, title, entry_capacity, winner_count, open_at, close_at, created_at) "
                        + "values (?, ?, 50, 5, ?, ?, now()) returning id",
                Long.class, productId, title, Timestamp.from(openAt), Timestamp.from(closeAt));
        assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();

        // When
        dropService.republish(ADMIN, dropId);

        // Then
        String key = String.valueOf(dropId);
        await().atMost(RECEIVE_TIMEOUT).until(() -> !recordsWithKey(key).isEmpty());
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> recordsWithKey(key).size() == 1);
        DropCreated message = JSON.readValue(recordsWithKey(key).getFirst().value(), DropCreated.class);
        assertThat(message).isEqualTo(new DropCreated(dropId, productId, title, 50, 5, openAt, closeAt));
    }
}
