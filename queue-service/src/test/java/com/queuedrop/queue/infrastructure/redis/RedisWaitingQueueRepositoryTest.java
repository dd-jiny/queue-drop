package com.queuedrop.queue.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.infrastructure.config.RedisScriptConfig;
import com.queuedrop.queue.infrastructure.redis.support.EntryRemainingFixture;
import com.queuedrop.queue.infrastructure.redis.support.RedisTestSupport;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class,
        RedisWaitingQueueRepository.class, RedisRemainingCapacityReader.class, SimpleMeterRegistry.class})
class RedisWaitingQueueRepositoryTest {

    private static final Long DROP_ID = 1L;
    private static final Duration TOKEN_TTL = Duration.ofSeconds(300);
    private static final long ONE_DAY_MS = Duration.ofDays(1).toMillis();

    @Autowired
    private RedisWaitingQueueRepository repository;

    @Autowired
    private StringRedisTemplate redis;

    /** 8단계: queue.admitted 카운터 확인용 */
    @Autowired
    private MeterRegistry meterRegistry;

    private EntryRemainingFixture entryRemaining;

    /**
     * 기준 시각. Redis 의 만료는 실제 시계로 동작하므로 고정 날짜가 아니라 실제 현재 시각(초 단위 내림)을 쓴다.
     * 서비스 메서드에는 now 인자로 명시 전달한다.
     */
    private Instant base;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
        entryRemaining = new EntryRemainingFixture(redis);
        base = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    }

    /** base 기준 오픈 중인 meta (base-1h ~ base+1h) */
    private QueueMeta openMeta(Long dropId) {
        return new QueueMeta(dropId, base.minus(Duration.ofHours(1)), base.plus(Duration.ofHours(1)));
    }

    private void enqueueAll(Long dropId, List<Long> userIds) {
        userIds.forEach(userId -> repository.enqueue(dropId, userId));
    }

    private static String waitingKey(Long dropId) {
        return "queue:" + dropId + ":waiting";
    }

    private static String seqKey(Long dropId) {
        return "queue:" + dropId + ":seq";
    }

    private static String metaKey(Long dropId) {
        return "queue:" + dropId + ":meta";
    }

    private static String tokenKey(Long dropId, Long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }

    @Nested
    @DisplayName("enqueue / rank")
    class Enqueue {

        @Test
        @DisplayName("[T3-12] A, B, C 순서로 진입하면 rank 0, 1, 2 이고 재진입해도 순번이 그대로다")
        void ranksFollowEntryOrder() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));

            // When
            EnqueueResult a = repository.enqueue(DROP_ID, 101L);
            EnqueueResult b = repository.enqueue(DROP_ID, 102L);
            EnqueueResult c = repository.enqueue(DROP_ID, 103L);
            EnqueueResult aAgain = repository.enqueue(DROP_ID, 101L);

            // Then
            assertThat(List.of(a, b, c)).containsExactly(
                    EnqueueResult.waiting(0), EnqueueResult.waiting(1), EnqueueResult.waiting(2));
            assertThat(aAgain).isEqualTo(EnqueueResult.waiting(0));
            assertThat(repository.rank(DROP_ID, 101L)).hasValue(0);
            assertThat(repository.rank(DROP_ID, 102L)).hasValue(1);
            assertThat(repository.rank(DROP_ID, 103L)).hasValue(2);
            assertThat(repository.rank(DROP_ID, 999L)).isEmpty();
            assertThat(redis.opsForValue().get(seqKey(DROP_ID))).isEqualTo("3");
        }

        @Test
        @DisplayName("[T3-13] 같은 사용자가 10개 스레드에서 동시에 진입해도 ZSet 멤버 1개, seq 1")
        void concurrentSameUserEnqueueIsIdempotent() throws Exception {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            int threads = 10;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<EnqueueResult>> futures = new ArrayList<>();

            // When
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < threads; i++) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        return repository.enqueue(DROP_ID, 7L);
                    }));
                }
                ready.await();
                start.countDown();
            }

            // Then
            for (Future<EnqueueResult> future : futures) {
                assertThat(future.get()).isEqualTo(EnqueueResult.waiting(0));
            }
            assertThat(redis.opsForZSet().zCard(waitingKey(DROP_ID))).isEqualTo(1L);
            assertThat(redis.opsForValue().get(seqKey(DROP_ID))).isEqualTo("1");
        }

        @Test
        @DisplayName("[T3-14] 토큰 보유 사용자가 진입하면 alreadyAdmitted, 대기열에 추가되지 않고 seq 도 증가하지 않는다")
        void tokenHolderIsAlreadyAdmitted() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            redis.opsForValue().set(tokenKey(DROP_ID, 7L), String.valueOf(base.toEpochMilli()), TOKEN_TTL);

            // When
            EnqueueResult result = repository.enqueue(DROP_ID, 7L);

            // Then
            assertThat(result).isEqualTo(EnqueueResult.alreadyAdmitted());
            assertThat(redis.opsForZSet().zCard(waitingKey(DROP_ID))).isZero();
            assertThat(redis.hasKey(seqKey(DROP_ID))).isFalse();
            assertThat(repository.hasToken(DROP_ID, 7L)).isTrue();
        }
    }

    @Nested
    @DisplayName("admit — 잔여 정원 확인 + ZPOPMIN + 토큰 발급")
    class Admit {

        @Test
        @DisplayName("[T3-15] 250명 대기, 잔여 1000, admit(n=100) 3회 → 100·100·50 입장, 진입 순서대로, 전원 토큰, 대기열 비어 있음")
        void admitsInBatchesInEntryOrder() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 1000);
            List<Long> users = LongStream.rangeClosed(1, 250).boxed().toList();
            enqueueAll(DROP_ID, users);

            // When
            List<Long> first = repository.admit(DROP_ID, 100, TOKEN_TTL, base);
            List<Long> second = repository.admit(DROP_ID, 100, TOKEN_TTL, base);
            List<Long> third = repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(first).hasSize(100);
            assertThat(second).hasSize(100);
            assertThat(third).hasSize(50);
            List<Long> admittedInOrder = new ArrayList<>(first);
            admittedInOrder.addAll(second);
            admittedInOrder.addAll(third);
            assertThat(admittedInOrder).containsExactlyElementsOf(users);
            assertThat(users).allSatisfy(userId -> {
                assertThat(repository.hasToken(DROP_ID, userId)).isTrue();
                assertThat(repository.rank(DROP_ID, userId)).isEmpty();
            });
            assertThat(redis.opsForZSet().zCard(waitingKey(DROP_ID))).isZero();
        }

        @Test
        @DisplayName("[T3-15] 토큰 값은 발급 시각 epoch ms 문자열이고 TTL 이 걸린다")
        void tokenValueIsIssuedAtMillis() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 10);
            repository.enqueue(DROP_ID, 7L);
            Instant issuedAt = base.plusMillis(123);

            // When
            repository.admit(DROP_ID, 10, TOKEN_TTL, issuedAt);

            // Then
            assertThat(redis.opsForValue().get(tokenKey(DROP_ID, 7L)))
                    .isEqualTo(String.valueOf(issuedAt.toEpochMilli()));
            assertThat(redis.getExpire(tokenKey(DROP_ID, 7L))).isBetween(1L, TOKEN_TTL.toSeconds());
        }

        @Test
        @DisplayName("[T3-16] 토큰 TTL 1초로 입장 → 만료 후 hasToken=false, rank 없음 (NOT_IN_QUEUE). 다시 진입하면 남은 대기자 뒤로 붙는다")
        void tokenExpires() {
            // Given — 7, 8, 9 순서로 대기, 7 만 TTL 1초로 입장
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 10);
            enqueueAll(DROP_ID, List.of(7L, 8L, 9L));
            repository.admit(DROP_ID, 1, Duration.ofSeconds(1), base);
            assertThat(repository.hasToken(DROP_ID, 7L)).isTrue();

            // When — 1.5초 안팎에 만료 (Awaitility, 최대 3초)
            await().atMost(Duration.ofSeconds(3)).pollInterval(Duration.ofMillis(100))
                    .until(() -> !repository.hasToken(DROP_ID, 7L));

            // Then — 만료 직후에는 대기열에도 없다
            assertThat(repository.rank(DROP_ID, 7L)).isEmpty();

            // When — 재진입
            EnqueueResult again = repository.enqueue(DROP_ID, 7L);

            // Then — 새 seq(4)로 남은 대기자 8, 9 뒤에 선다 (원래 순번을 되찾지 않는다)
            assertThat(again).isEqualTo(EnqueueResult.waiting(2));
            assertThat(repository.rank(DROP_ID, 8L)).hasValue(0);
            assertThat(repository.rank(DROP_ID, 7L)).hasValue(2);
            assertThat(redis.opsForValue().get(seqKey(DROP_ID))).isEqualTo("4");
        }

        @Test
        @DisplayName("[T3-17] 잔여 정원 0 이면 아무도 입장하지 않고 대기열이 유지된다")
        void soldOutAdmitsNobody() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.soldOut(DROP_ID);
            enqueueAll(DROP_ID, List.of(1L, 2L, 3L));

            // When
            List<Long> admitted = repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(admitted).isEmpty();
            assertThat(redis.opsForZSet().zCard(waitingKey(DROP_ID))).isEqualTo(3L);
            assertThat(repository.rank(DROP_ID, 1L)).hasValue(0);
            assertThat(repository.hasToken(DROP_ID, 1L)).isFalse();
        }

        @Test
        @DisplayName("[T3-18] 잔여 정원 키가 없으면 아무도 입장하지 않고 대기열이 유지된다")
        void remainingKeyMissingAdmitsNobody() {
            // Given — entry 잔여 키 없음
            repository.saveMeta(openMeta(DROP_ID));
            enqueueAll(DROP_ID, List.of(1L, 2L));

            // When
            List<Long> admitted = repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(admitted).isEmpty();
            assertThat(redis.opsForZSet().zCard(waitingKey(DROP_ID))).isEqualTo(2L);
            assertThat(repository.hasToken(DROP_ID, 1L)).isFalse();
        }

        @Test
        @DisplayName("[T3-15] 대기열이 비어 있으면 빈 목록을 반환한다")
        void emptyQueueAdmitsNobody() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 10);

            // When
            List<Long> admitted = repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(admitted).isEmpty();
        }

        @Test
        @DisplayName("[T3-15] 드로우별 대기열·토큰은 서로 독립이다 (한 드로우 입장이 다른 드로우에 영향 없음)")
        void dropsAreIsolated() {
            // Given — 같은 사용자 7 이 드로우 1·2 에 모두 대기
            repository.saveMeta(openMeta(1L));
            repository.saveMeta(openMeta(2L));
            entryRemaining.initialize(1L, 10);
            entryRemaining.initialize(2L, 10);
            enqueueAll(1L, List.of(7L));
            enqueueAll(2L, List.of(8L, 7L));

            // When
            List<Long> admitted = repository.admit(1L, 10, TOKEN_TTL, base);

            // Then
            assertThat(admitted).containsExactly(7L);
            assertThat(repository.hasToken(1L, 7L)).isTrue();
            assertThat(repository.hasToken(2L, 7L)).isFalse();
            assertThat(repository.rank(2L, 7L)).hasValue(1);
            assertThat(repository.enqueue(2L, 7L)).isEqualTo(EnqueueResult.waiting(1));
        }
    }

    @Nested
    @DisplayName("활성 드로우 인덱스")
    class ActiveDrops {

        @Test
        @DisplayName("[T3-19] findActiveDropIds 는 closeAt > now 인 드로우만, removeExpiredDrops 는 closeAt <= now 를 제거한다")
        void activeDropsByCloseAt() {
            // Given — 1: 마감 1시간 뒤, 2: 마감 == now, 3: 마감 1ms 전
            Instant now = base;
            repository.saveMeta(new QueueMeta(1L, now.minus(Duration.ofHours(1)), now.plus(Duration.ofHours(1))));
            repository.saveMeta(new QueueMeta(2L, now.minus(Duration.ofHours(1)), now));
            repository.saveMeta(new QueueMeta(3L, now.minus(Duration.ofHours(1)), now.minusMillis(1)));

            // When
            List<Long> active = repository.findActiveDropIds(now);
            repository.removeExpiredDrops(now);

            // Then
            assertThat(active).containsExactly(1L);
            assertThat(redis.opsForZSet().range("queue:active-drops", 0, -1)).containsExactly("1");
        }

        @Test
        @DisplayName("[T3-19] closeAt 1ms 전 시각에는 활성이다 (경계)")
        void activeJustBeforeClose() {
            // Given
            Instant closeAt = base.plus(Duration.ofHours(1));
            repository.saveMeta(new QueueMeta(DROP_ID, base, closeAt));

            // When
            List<Long> active = repository.findActiveDropIds(closeAt.minusMillis(1));
            repository.removeExpiredDrops(closeAt.minusMillis(1));

            // Then
            assertThat(active).containsExactly(DROP_ID);
            assertThat(redis.opsForZSet().range("queue:active-drops", 0, -1)).containsExactly(String.valueOf(DROP_ID));
        }
    }

    @Nested
    @DisplayName("meta 저장과 키 정리 (v1.2)")
    class MetaAndExpiry {

        @Test
        @DisplayName("[T3-25] saveMeta 후 findMeta 로 같은 값을 읽고, 없는 드로우는 empty")
        void saveAndFindMeta() {
            // Given
            QueueMeta meta = openMeta(DROP_ID);

            // When
            repository.saveMeta(meta);

            // Then
            assertThat(repository.findMeta(DROP_ID)).contains(meta);
            assertThat(repository.findMeta(999L)).isEmpty();
        }

        @Test
        @DisplayName("[T3-25] saveMeta 후 meta 의 만료 시각 = closeAt + 1일")
        void metaExpiresOneDayAfterClose() {
            // Given
            QueueMeta meta = openMeta(DROP_ID);

            // When
            repository.saveMeta(meta);

            // Then
            assertThat(RedisTestSupport.pexpireTime(redis, metaKey(DROP_ID)))
                    .isEqualTo(meta.closeAt().toEpochMilli() + ONE_DAY_MS);
        }

        @Test
        @DisplayName("[T3-25] enqueue 후 waiting·seq 도 closeAt + 1일에 만료된다")
        void waitingAndSeqExpireWithMeta() {
            // Given
            QueueMeta meta = openMeta(DROP_ID);
            repository.saveMeta(meta);
            long expected = meta.closeAt().toEpochMilli() + ONE_DAY_MS;

            // When
            repository.enqueue(DROP_ID, 7L);

            // Then
            assertThat(RedisTestSupport.pexpireTime(redis, waitingKey(DROP_ID))).isEqualTo(expected);
            assertThat(RedisTestSupport.pexpireTime(redis, seqKey(DROP_ID))).isEqualTo(expected);
        }

        @Test
        @DisplayName("[T3-25] 대기열이 admit 으로 비었다가(키 삭제) 다시 enqueue 하면 waiting 에 만료가 다시 걸린다")
        void expiryReappliedAfterQueueEmptied() {
            // Given
            QueueMeta meta = openMeta(DROP_ID);
            repository.saveMeta(meta);
            entryRemaining.initialize(DROP_ID, 10);
            repository.enqueue(DROP_ID, 1L);
            repository.admit(DROP_ID, 10, TOKEN_TTL, base);
            assertThat(redis.hasKey(waitingKey(DROP_ID))).isFalse();

            // When
            EnqueueResult result = repository.enqueue(DROP_ID, 2L);

            // Then — seq 는 이어서 증가, rank 는 0
            assertThat(result).isEqualTo(EnqueueResult.waiting(0));
            assertThat(redis.opsForValue().get(seqKey(DROP_ID))).isEqualTo("2");
            assertThat(RedisTestSupport.pexpireTime(redis, waitingKey(DROP_ID)))
                    .isEqualTo(meta.closeAt().toEpochMilli() + ONE_DAY_MS);
        }

        @Test
        @DisplayName("[T3-25] meta 가 없을 때 enqueue 하면 waiting·seq 에 만료가 걸리지 않는다 (-1)")
        void noExpiryWithoutMeta() {
            // Given — meta 없음.
            // 저장소 단위의 방어 동작 확인이다. 실제 enter 경로에서는 QueueService 가 meta 없음을 먼저
            // 404(QUEUE_DROP_NOT_FOUND)로 막으므로 이 상태로 enqueue.lua 에 도달하지 않는다.

            // When
            repository.enqueue(DROP_ID, 7L);

            // Then
            assertThat(RedisTestSupport.pexpireTime(redis, waitingKey(DROP_ID))).isEqualTo(-1L);
            assertThat(RedisTestSupport.pexpireTime(redis, seqKey(DROP_ID))).isEqualTo(-1L);
        }

        @Test
        @DisplayName("[T3-25] closeAt + 1일이 이미 지난 드로우를 저장하면 meta 는 즉시 만료되고 활성 드로우도 아니다")
        void longClosedDropIsNotKept() {
            // Given — 2일 전에 마감된 드로우 (drop.created 재전송 등)
            QueueMeta old = new QueueMeta(DROP_ID,
                    base.minus(Duration.ofDays(3)), base.minus(Duration.ofDays(2)));

            // When
            repository.saveMeta(old);

            // Then
            assertThat(repository.findMeta(DROP_ID)).isEmpty();
            assertThat(repository.findActiveDropIds(base)).isEmpty();
        }

        @Test
        @DisplayName("[T3-25] 같은 meta 를 다시 저장해도 기존 대기열은 그대로다 (멱등)")
        void resaveMetaKeepsWaiting() {
            // Given
            QueueMeta meta = openMeta(DROP_ID);
            repository.saveMeta(meta);
            enqueueAll(DROP_ID, List.of(1L, 2L));

            // When
            repository.saveMeta(meta);

            // Then
            assertThat(repository.findMeta(DROP_ID)).contains(meta);
            assertThat(repository.rank(DROP_ID, 2L)).hasValue(1);
            assertThat(redis.opsForValue().get(seqKey(DROP_ID))).isEqualTo("2");
        }
    }

    @Nested
    @DisplayName("입장 메트릭 queue.admitted (8단계 3.2)")
    class AdmittedMetric {

        /** 태그 유무와 무관하게 이름 기준 합계 (단계 문서는 태그를 정하지 않았다) */
        private double admitted() {
            return meterRegistry.find("queue.admitted").counters().stream().mapToDouble(Counter::count).sum();
        }

        @Test
        @DisplayName("[8단계 메트릭] 250명 대기, admit(n=100) 3회 → queue.admitted 는 실제 입장 인원 합 250 만큼 증가 (100·100·50)")
        void countsAdmittedUsers() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 1000);
            enqueueAll(DROP_ID, LongStream.rangeClosed(1, 250).boxed().toList());
            double before = admitted();

            // When
            repository.admit(DROP_ID, 100, TOKEN_TTL, base);
            double afterFirst = admitted();
            repository.admit(DROP_ID, 100, TOKEN_TTL, base);
            repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(afterFirst - before).isEqualTo(100.0);
            assertThat(admitted() - before).as("요청 n 이 아니라 반환 인원만큼").isEqualTo(250.0);
        }

        @Test
        @DisplayName("[8단계 메트릭] 잔여 정원 0 이면 입장 0명 → queue.admitted 증가 없음 (T8-07 소진 후 증가 멈춤)")
        void soldOutDoesNotCount() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.soldOut(DROP_ID);
            enqueueAll(DROP_ID, List.of(1L, 2L, 3L));
            double before = admitted();

            // When
            List<Long> result = repository.admit(DROP_ID, 100, TOKEN_TTL, base);

            // Then
            assertThat(result).isEmpty();
            assertThat(admitted()).isEqualTo(before);
        }

        @Test
        @DisplayName("[8단계 메트릭] 요청 n(10)이 대기 인원(5)보다 커도 반환된 입장 인원만큼만 증가")
        void countsReturnedSizeNotRequestedN() {
            // Given
            repository.saveMeta(openMeta(DROP_ID));
            entryRemaining.initialize(DROP_ID, 1000);
            enqueueAll(DROP_ID, List.of(1L, 2L, 3L, 4L, 5L));
            double before = admitted();

            // When
            List<Long> result = repository.admit(DROP_ID, 10, TOKEN_TTL, base);

            // Then
            assertThat(result).hasSize(5);
            assertThat(admitted() - before).as("요청 n(10)이 아니라 반환 인원(5)").isEqualTo(5.0);
        }
    }
}
