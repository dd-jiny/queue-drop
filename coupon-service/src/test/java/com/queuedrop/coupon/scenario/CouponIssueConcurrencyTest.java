package com.queuedrop.coupon.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.service.CouponIssueService;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 동시 발급 시나리오 (단계 문서 4장 T6-13, T6-14; NFR-01, NFR-02).
 * HTTP 를 거치면 톰캣 스레드 풀이 동시성을 줄여서 application 서비스를 직접 호출한다.
 * 플랫폼 스레드 풀(threads 개)에 작업을 모두 넣고 start 래치로 한꺼번에 출발시킨다.
 */
class CouponIssueConcurrencyTest extends CouponScenarioTestBase {

    private static final Logger log = LoggerFactory.getLogger(CouponIssueConcurrencyTest.class);

    @Autowired CouponIssueService issueService;

    private record Outcome(int success, Map<ErrorCode, Integer> failures, Queue<Throwable> unexpected, long elapsedMs) {
        int failuresOf(CouponErrorCode code) {
            return failures.getOrDefault(code, 0);
        }
    }

    /** tasks 개 발급 요청을 threads 개 스레드로 동시에 실행한다. i 번째 요청은 userOf(i) 사용자 */
    private Outcome issueConcurrently(long couponEventId, int tasks, int threads, LongUnaryOperator userOf)
            throws Exception {
        AtomicInteger success = new AtomicInteger();
        Map<ErrorCode, AtomicInteger> failures = new ConcurrentHashMap<>();
        Queue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(tasks);
        long elapsedMs;

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < tasks; i++) {
                long userId = userOf.applyAsLong(i);
                pool.submit(() -> {
                    try {
                        start.await();                                     // 한꺼번에 출발
                        issueService.issue(new CouponIssueCommand(couponEventId, userId));
                        success.incrementAndGet();
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
            long startedAt = System.nanoTime();
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).as("모든 발급 요청이 60초 안에 끝났다").isTrue();
            elapsedMs = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        }

        Map<ErrorCode, Integer> counts = failures.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
        return new Outcome(success.get(), counts, unexpected, elapsedMs);
    }

    @Test
    @DisplayName("[T6-13] 수량 100, 서로 다른 사용자 1,000명이 32스레드로 동시 발급하면 성공 정확히 100, SOLD_OUT 900, Redis 잔여 0·Set 100, 최종 DB 100건")
    void thousandUsersCompeteForHundredCoupons() throws Exception {
        // Given — 수량 100 쿠폰 이벤트, 사용자 1~1,000
        int quantity = 100;
        int users = 1_000;
        long eventId = givenOpenEvent(quantity);

        // When — 32스레드로 동시 발급
        Outcome outcome = issueConcurrently(eventId, users, 32, i -> i + 1);

        // Then — 판정 결과
        assertThat(outcome.unexpected()).as("ApplicationException 외 예외 없음").isEmpty();
        assertThat(outcome.success()).isEqualTo(quantity);
        assertThat(outcome.failuresOf(CouponErrorCode.SOLD_OUT)).isEqualTo(users - quantity);
        assertThat(outcome.failures()).containsOnlyKeys(CouponErrorCode.SOLD_OUT);

        // Then — Redis
        assertThat(remaining(eventId)).isEqualTo("0");
        assertThat(issuedCount(eventId)).isEqualTo(quantity);

        // Then — 비동기 저장 완료 후 DB 100건 (Redis 발급자와 같은 사용자들)
        long consumeStartedAt = System.nanoTime();
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200)).until(() -> dbCount(eventId) == quantity);
        awaitAllIssuedEventsConsumed();
        long consumeMs = Duration.ofNanos(System.nanoTime() - consumeStartedAt).toMillis();
        assertThat(dbCount(eventId)).isEqualTo(quantity);
        assertThat(jdbc.queryForList("SELECT user_id FROM issued_coupons WHERE coupon_event_id = ?",
                        String.class, eventId))
                .containsExactlyInAnyOrderElementsOf(redis.opsForSet().members("coupon:" + eventId + ":issued"));

        // 8단계 비교 기준 (단계 문서 리스크 "Kafka 응답 지연")
        log.info("[T6-13] 발급 {}건 / 32스레드: 판정+발행 {} ms ({} ms/건), 이후 DB 저장 완료까지 {} ms",
                users, outcome.elapsedMs(), String.format("%.2f", (double) outcome.elapsedMs() / users), consumeMs);
    }

    @Test
    @DisplayName("[T6-14] 같은 사용자가 10스레드로 동시 발급하면 1번만 성공, 9번 DUPLICATE, 잔여 99, 최종 DB 1건")
    void sameUserTenTimesConcurrently() throws Exception {
        // Given — 수량 100 쿠폰 이벤트, 사용자 1명
        long eventId = givenOpenEvent(100);
        long userId = 7L;

        // When — 같은 사용자로 10개 요청을 10스레드로 동시 출발
        Outcome outcome = issueConcurrently(eventId, 10, 10, i -> userId);

        // Then — 판정 결과
        assertThat(outcome.unexpected()).as("ApplicationException 외 예외 없음").isEmpty();
        assertThat(outcome.success()).isEqualTo(1);
        assertThat(outcome.failuresOf(CouponErrorCode.DUPLICATE)).isEqualTo(9);
        assertThat(outcome.failures()).containsOnlyKeys(CouponErrorCode.DUPLICATE);

        // Then — Redis: 한 자리만 차감
        assertThat(remaining(eventId)).isEqualTo("99");
        assertThat(issuedCount(eventId)).isEqualTo(1);

        // Then — DB 1건
        await().atMost(CONSUME_WAIT).pollInterval(Duration.ofMillis(200)).until(() -> dbCount(eventId) == 1);
        awaitAllIssuedEventsConsumed();
        assertThat(dbCount(eventId, userId)).isEqualTo(1);
        assertThat(dbCount(eventId)).isEqualTo(1);
    }
}
