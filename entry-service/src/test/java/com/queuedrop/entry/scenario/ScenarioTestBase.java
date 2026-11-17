package com.queuedrop.entry.scenario;

import static com.queuedrop.entry.support.KafkaTopicReadiness.awaitTopicMetadata;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.entry.application.dto.DropSnapshotRegisterCommand;
import com.queuedrop.entry.application.service.DropSnapshotRegisterService;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 시나리오 테스트 공통 설정. 모든 시나리오 클래스가 이 클래스를 상속해 같은 컨텍스트를 쓴다
 * (@AutoConfigureMockMvc 도 공통으로 둬서 컨텍스트 캐시 키를 맞춘다).
 * 테스트 기본값(src/test/resources/config/application.yml)은 스케줄러 꺼짐이지만, T5-21 등록 검증을 위해 켠다.
 * 자동 실행은 NoOpTaskScheduler 가 막는다.
 */
@SpringBootTest(properties = "entry.draw.scheduler-enabled=true")
@AutoConfigureMockMvc
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
        ScenarioTestSupport.Config.class})
abstract class ScenarioTestBase {

    /** 비동기 저장(entry.created 소비) 대기 상한 */
    static final Duration CONSUME_WAIT = Duration.ofSeconds(30);
    private static volatile boolean producerWarmedUp;

    @Autowired StringRedisTemplate redis;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired DropSnapshotRegisterService registerService;
    @Autowired ScenarioTestSupport.ControllableEntryEventPublisher publisher;
    @Autowired ScenarioTestSupport.RecordingDrawEventPublisher drawPublisher;

    @BeforeEach
    void resetState() throws Exception {
        publisher.reset();
        drawPublisher.reset();
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE entries, drop_snapshots, draw_results");
        warmUpProducer();
    }

    /**
     * 첫 send 의 메타데이터 조회가 max.block.ms(1초)에 걸려 동시성 테스트가 흔들리지 않게 미리 한 번 보낸다.
     * dropId 0 은 응모자가 없어 저장 리스너가 무시한다.
     */
    private void warmUpProducer() throws Exception {
        if (producerWarmedUp) {
            return;
        }
        awaitTopicMetadata(kafkaTemplate, Topics.ENTRY_CREATED, Topics.DRAW_COMPLETED);   // NewTopic 직후 메타데이터 전파 대기
        kafkaTemplate.send(Topics.ENTRY_CREATED, "0:0",
                        new EntryCreated(UUID.randomUUID().toString(), 0L, 0L, 1, ScenarioTestSupport.NOW))
                .get(10, TimeUnit.SECONDS);
        producerWarmedUp = true;
    }

    void givenOpenDrop(long dropId, int capacity) {
        registerService.register(new DropSnapshotRegisterCommand(
                dropId, capacity, 1, ScenarioTestSupport.OPEN_AT, ScenarioTestSupport.CLOSE_AT));
    }

    /** queue-service 가 발급하는 입장 토큰 키를 직접 심는다 (Lua 는 EXISTS 만 본다) */
    void givenToken(long dropId, long userId) {
        redis.opsForValue().set("queue:" + dropId + ":token:" + userId,
                String.valueOf(ScenarioTestSupport.NOW.toEpochMilli()), Duration.ofMinutes(5));
    }

    String remaining(long dropId) {
        return redis.opsForValue().get("entry:" + dropId + ":remaining");
    }

    long applicantCount(long dropId) {
        return redis.opsForSet().size("entry:" + dropId + ":applicants");
    }

    boolean isApplicant(long dropId, long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember("entry:" + dropId + ":applicants", String.valueOf(userId)));
    }

    long dbCount(long dropId) {
        return jdbc.queryForObject("SELECT count(*) FROM entries WHERE drop_id = ?", Long.class, dropId);
    }

    Integer dbSeq(long dropId, long userId) {
        return jdbc.query("SELECT entry_seq FROM entries WHERE drop_id = ? AND user_id = ?",
                rs -> rs.next() ? rs.getInt(1) : null, dropId, userId);
    }

    /** 5단계: 마감 시각(PAST_CLOSE_AT)이 NOW 보다 앞선 OPEN 드로우. drop.created 수신과 같은 경로로 DB·Redis 를 만든다 */
    void givenDueDrop(long dropId, int capacity, int winnerCount) {
        registerService.register(new DropSnapshotRegisterCommand(
                dropId, capacity, winnerCount, ScenarioTestSupport.PAST_OPEN_AT, ScenarioTestSupport.PAST_CLOSE_AT));
    }

    /** 5단계: 판정 성공한 응모자(Redis applicants) userId fromUser ~ toUser */
    void givenRedisApplicants(long dropId, long fromUser, long toUser) {
        String[] users = LongStream.rangeClosed(fromUser, toUser).mapToObj(String::valueOf).toArray(String[]::new);
        redis.opsForSet().add("entry:" + dropId + ":applicants", users);
    }

    /** 5단계: 저장 컨슈머가 저장을 마친 응모 (APPLIED). entry_seq = userId */
    void givenSavedEntries(long dropId, long fromUser, long toUser) {
        jdbc.batchUpdate("INSERT INTO entries (drop_id, user_id, entry_seq, status, applied_at) VALUES (?, ?, ?, 'APPLIED', ?)",
                LongStream.rangeClosed(fromUser, toUser)
                        .mapToObj(u -> new Object[]{dropId, u, (int) u,
                                java.sql.Timestamp.from(ScenarioTestSupport.PAST_OPEN_AT.plusSeconds(u))})
                        .toList());
    }

    String snapshotStatus(long dropId) {
        return jdbc.queryForObject("SELECT status FROM drop_snapshots WHERE drop_id = ?", String.class, dropId);
    }

    long drawResultCount(long dropId) {
        return jdbc.queryForObject("SELECT count(*) FROM draw_results WHERE drop_id = ?", Long.class, dropId);
    }

    long entryCountByStatus(long dropId, String status) {
        return jdbc.queryForObject("SELECT count(*) FROM entries WHERE drop_id = ? AND status = ?", Long.class,
                dropId, status);
    }

    /** entry.created 를 모두 소비할 때까지 기다린다 (늦게 처리되는 이벤트까지 반영된 뒤 단언하려고) */
    void awaitAllEntryEventsConsumed() {
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200))
                .until(() -> ScenarioTestSupport.entryCreatedFullyConsumed(kafkaAdmin));
    }
}
