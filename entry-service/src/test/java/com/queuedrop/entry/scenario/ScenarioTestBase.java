package com.queuedrop.entry.scenario;

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
 */
@SpringBootTest
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

    @BeforeEach
    void resetState() throws Exception {
        publisher.reset();
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
        jdbc.update("TRUNCATE TABLE entries, drop_snapshots");
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

    /** entry.created 를 모두 소비할 때까지 기다린다 (늦게 처리되는 이벤트까지 반영된 뒤 단언하려고) */
    void awaitAllEntryEventsConsumed() {
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200))
                .until(() -> ScenarioTestSupport.entryCreatedFullyConsumed(kafkaAdmin));
    }
}
