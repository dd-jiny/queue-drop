package com.queuedrop.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.application.config.AdmissionProperties;
import com.queuedrop.queue.application.service.AdmissionService;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import com.queuedrop.queue.infrastructure.config.RedisScriptConfig;
import com.queuedrop.queue.infrastructure.redis.RedisSchedulerGate;
import com.queuedrop.queue.infrastructure.redis.RedisWaitingQueueRepository;
import com.queuedrop.queue.infrastructure.redis.support.EntryRemainingFixture;
import com.queuedrop.queue.infrastructure.redis.support.RedisTestSupport;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 다중 인스턴스 입장 처리: AdmissionService 2개가 각자 다른 RedisSchedulerGate(인스턴스 ID)를 갖고
 * 같은 Redis 를 공유할 때, 한 주기에 전체에서 정확히 N 명만 입장한다.
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisWaitingQueueRepository.class})
class AdmissionConcurrencyIntegrationTest {

    private static final Long DROP_ID = 1L;
    private static final int WAITING_USERS = 500;
    private static final int BATCH_SIZE = 100;
    private static final AdmissionProperties PROPS = new AdmissionProperties(1_000, BATCH_SIZE, 300);

    @Autowired
    private WaitingQueueRepository queueRepository;

    @Autowired
    private StringRedisTemplate redis;

    private AdmissionService instanceA;
    private AdmissionService instanceB;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
        // Redis 만료는 실제 시계로 동작하므로 기준 시각은 실제 현재 시각, 서비스에는 고정 Clock 으로 주입
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        instanceA = new AdmissionService(queueRepository, new RedisSchedulerGate(redis, "instance-A"), PROPS, clock);
        instanceB = new AdmissionService(queueRepository, new RedisSchedulerGate(redis, "instance-B"), PROPS, clock);

        queueRepository.saveMeta(new QueueMeta(DROP_ID, now.minus(Duration.ofHours(1)), now.plus(Duration.ofHours(1))));
        new EntryRemainingFixture(redis).initialize(DROP_ID, 10_000);
        LongStream.rangeClosed(1, WAITING_USERS).forEach(userId -> queueRepository.enqueue(DROP_ID, userId));
    }

    private List<Integer> admitConcurrently(AdmissionService... services) throws Exception {
        CountDownLatch ready = new CountDownLatch(services.length);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> futures;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Integer>> tasks = List.of(services).stream()
                    .<Callable<Integer>>map(service -> () -> {
                        ready.countDown();
                        start.await();
                        return service.admit();
                    })
                    .toList();
            futures = tasks.stream().map(executor::submit).toList();
            ready.await();
            start.countDown();
        }
        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    private long tokenHolderCount() {
        return LongStream.rangeClosed(1, WAITING_USERS)
                .filter(userId -> queueRepository.hasToken(DROP_ID, userId))
                .count();
    }

    @Test
    @DisplayName("[T3-21] 대기 500명, n=100, 인스턴스 2개가 동시에 admit → 이번 주기 입장 총 100명, 한 인스턴스만 100")
    void onlyOneInstanceAdmitsPerCycle() throws Exception {
        // When
        List<Integer> results = admitConcurrently(instanceA, instanceB);

        // Then
        assertThat(results).containsExactlyInAnyOrder(BATCH_SIZE, 0);
        assertThat(tokenHolderCount()).isEqualTo(BATCH_SIZE);
        assertThat(queueRepository.rank(DROP_ID, 101L)).hasValue(0);   // 진입 순서 앞 100명이 입장
        assertThat(redis.opsForZSet().zCard("queue:" + DROP_ID + ":waiting")).isEqualTo(400L);
    }

    @Test
    @DisplayName("[T3-21] 게이트 window 가 지나면 다음 주기에 다음 100명이 입장한다 (주기당 1회)")
    void nextCycleAdmitsNextBatch() throws Exception {
        // Given — 첫 주기
        admitConcurrently(instanceA, instanceB);

        // When — window(1초) 만료 후 다음 주기
        await().pollDelay(Duration.ofMillis(1_100)).atMost(Duration.ofSeconds(3))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> !Boolean.TRUE.equals(redis.hasKey("queue:admission:gate")));
        List<Integer> results = admitConcurrently(instanceA, instanceB);

        // Then
        assertThat(results).containsExactlyInAnyOrder(BATCH_SIZE, 0);
        assertThat(tokenHolderCount()).isEqualTo(2L * BATCH_SIZE);
        assertThat(queueRepository.rank(DROP_ID, 201L)).hasValue(0);
    }
}
