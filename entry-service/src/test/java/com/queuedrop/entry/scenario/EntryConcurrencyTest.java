package com.queuedrop.entry.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.service.EntryApplyService;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 동시성 시나리오 (단계 문서 4.3, T4-28, T4-29).
 * HTTP 를 거치면 톰캣 스레드 풀이 동시성을 줄여서 application 서비스를 직접 호출한다.
 * 가상 스레드: 작업마다 스레드가 생겨 전원이 start 래치에서 대기할 수 있다.
 */
class EntryConcurrencyTest extends ScenarioTestBase {

    @Autowired EntryApplyService entryApplyService;

    /** 동시 응모 결과 집계 */
    private record Outcome(int success, Map<ErrorCode, Integer> failures, List<Integer> seqs, Queue<Throwable> unexpected) {
        int failuresOf(EntryErrorCode code) {
            return failures.getOrDefault(code, 0);
        }
    }

    /** tasks 개 작업을 동시에 출발시킨다. i 번째 작업은 userOf(i) 사용자로 응모한다 */
    private Outcome applyConcurrently(long dropId, int tasks, LongUnaryOperator userOf) throws Exception {
        AtomicInteger success = new AtomicInteger();
        Map<ErrorCode, AtomicInteger> failures = new ConcurrentHashMap<>();
        Queue<Integer> seqs = new ConcurrentLinkedQueue<>();
        Queue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(tasks);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(tasks);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < tasks; i++) {
                long userId = userOf.applyAsLong(i);
                pool.submit(() -> {
                    try {
                        ready.countDown();
                        start.await();                                     // 전원 동시에 출발
                        EntryApplyResult r = entryApplyService.apply(new EntryApplyCommand(dropId, userId));
                        success.incrementAndGet();
                        seqs.add(r.entrySeq());
                    } catch (ApplicationException e) {
                        failures.computeIfAbsent(e.errorCode(), k -> new AtomicInteger()).incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        unexpected.add(e);
                    } catch (Throwable t) {
                        unexpected.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).as("모든 작업이 출발선에 섰다").isTrue();
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).as("모든 응모가 60초 안에 끝났다").isTrue();
        }

        Map<ErrorCode, Integer> failureCounts = failures.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
        return new Outcome(success.get(), failureCounts, List.copyOf(seqs), unexpected);
    }

    @Test
    @DisplayName("[T4-28] 정원 100, 토큰 보유자 1,000명 동시 응모 시 정확히 100명 성공, 900명 SOLD_OUT, remaining 0, seq 1~100 중복 없음, 최종 DB 100건")
    void thousandUsersCompeteForHundredSeats() throws Exception {
        // Given — 정원 100 드로우, 사용자 1,000명 모두 입장 토큰 보유
        long dropId = 401L;
        int capacity = 100;
        int users = 1_000;
        givenOpenDrop(dropId, capacity);
        LongStream.rangeClosed(1, users).forEach(u -> givenToken(dropId, u));

        // When — 1,000명 동시 응모
        Outcome outcome = applyConcurrently(dropId, users, i -> i + 1);

        // Then — 판정 결과
        assertThat(outcome.unexpected()).as("ApplicationException 외 예외 없음").isEmpty();
        assertThat(outcome.success()).isEqualTo(capacity);
        assertThat(outcome.failuresOf(EntryErrorCode.SOLD_OUT)).isEqualTo(users - capacity);
        assertThat(outcome.failures()).containsOnlyKeys(EntryErrorCode.SOLD_OUT);
        assertThat(Set.copyOf(outcome.seqs()))
                .as("seq 100개 모두 다르고 1~100 이다")
                .hasSize(capacity)
                .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, capacity).boxed().toList());

        // Then — Redis
        assertThat(remaining(dropId)).isEqualTo("0");
        assertThat(applicantCount(dropId)).isEqualTo(capacity);

        // Then — 비동기 저장 완료 후 DB 100건, seq 도 1~100 중복 없음
        awaitAllEntryEventsConsumed();
        assertThat(dbCount(dropId)).isEqualTo(capacity);
        assertThat(jdbc.queryForList("SELECT entry_seq FROM entries WHERE drop_id = ?", Integer.class, dropId))
                .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, capacity).boxed().toList());
    }

    @Test
    @DisplayName("[T4-29] 같은 사용자가 10번 동시에 응모하면 1번만 성공, 9번 DUPLICATE, remaining 99, 최종 DB 1건")
    void sameUserTenTimesConcurrently() throws Exception {
        // Given — 정원 100 드로우, 사용자 1명 토큰 보유
        long dropId = 402L;
        long userId = 7L;
        givenOpenDrop(dropId, 100);
        givenToken(dropId, userId);

        // When — 같은 사용자로 10개 작업 동시 출발
        Outcome outcome = applyConcurrently(dropId, 10, i -> userId);

        // Then — 판정 결과
        assertThat(outcome.unexpected()).as("ApplicationException 외 예외 없음").isEmpty();
        assertThat(outcome.success()).isEqualTo(1);
        assertThat(outcome.failuresOf(EntryErrorCode.DUPLICATE)).isEqualTo(9);
        assertThat(outcome.failures()).containsOnlyKeys(EntryErrorCode.DUPLICATE);
        assertThat(outcome.seqs()).containsExactly(1);

        // Then — Redis: 한 자리만 차감
        assertThat(remaining(dropId)).isEqualTo("99");
        assertThat(applicantCount(dropId)).isEqualTo(1);

        // Then — DB 1건
        awaitAllEntryEventsConsumed();
        assertThat(dbCount(dropId)).isEqualTo(1);
        assertThat(dbSeq(dropId, userId)).isEqualTo(1);
    }
}
