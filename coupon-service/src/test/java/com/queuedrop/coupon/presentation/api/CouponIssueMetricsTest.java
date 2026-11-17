package com.queuedrop.coupon.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.willThrow;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.service.CouponIssueService;
import com.queuedrop.coupon.application.service.CouponQueryService;
import com.queuedrop.coupon.application.service.fake.FakeCouponEventPublisher;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 쿠폰 발급 판정 결과 카운터 coupon.issue.outcome{outcome} (단계 문서 8단계 3.2 "관찰할 메트릭").
 * 태그 값은 IssueOutcome 상수명(OK/NOT_STARTED/DUPLICATE/SOLD_OUT/NOT_FOUND) + TEMPORARILY_FAILED.
 * 응답 code(COUPON_EVENT_NOT_FOUND 등)와 다르다. 기록 위치는 presentation — 실제 CouponIssueService + Fake 포트로 검증.
 * domain 타입(IssueOutcome)은 import 하지 않는다.
 */
@WebMvcTest(CouponController.class)
@Import({GlobalExceptionHandler.class, CouponIssueMetricsTest.Config.class})
class CouponIssueMetricsTest {

    private static final String METRIC = "coupon.issue.outcome";
    private static final Instant NOW = Instant.parse("2026-12-01T01:00:00Z");
    /** 테스트마다 다른 이벤트를 써서 공유 Fake 상태가 섞이지 않게 한다 */
    private static final AtomicLong EVENT_IDS = new AtomicLong(1000);

    @TestConfiguration
    static class Config {
        @Bean FakeCouponGate fakeCouponGate() { return new FakeCouponGate(); }
        @Bean FakeCouponEventPublisher fakeCouponEventPublisher() { return new FakeCouponEventPublisher(); }
        @Bean Clock clock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
        @Bean CouponIssueService couponIssueService(FakeCouponGate gate, FakeCouponEventPublisher publisher, Clock clock) {
            return new CouponIssueService(gate, publisher, clock);
        }
    }

    @Autowired MockMvc mockMvc;
    @Autowired MeterRegistry meterRegistry;
    @Autowired FakeCouponGate gate;
    @Autowired FakeCouponEventPublisher publisher;
    @MockitoBean CouponQueryService queryService;

    // ---- 도우미 ----
    private long startedEvent(int quantity) {
        long eventId = EVENT_IDS.incrementAndGet();
        gate.initialize(eventId, quantity, NOW.minus(Duration.ofMinutes(1)));
        return eventId;
    }

    private ResultActions issue(long eventId, long userId) throws Exception {
        return mockMvc.perform(post("/api/coupons/events/{eventId}/issue", eventId).header("X-User-Id", userId));
    }

    private double count(String outcome) {
        Counter counter = meterRegistry.find(METRIC).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    /** 잘못된 태그 값은 누적값과 무관하게 카운터 자체가 없어야 한다 (순서 의존 없음) */
    private void assertNoTag(String outcome) {
        assertThat(meterRegistry.find(METRIC).tag("outcome", outcome).counter())
                .as("ErrorCode 이름 태그 outcome=%s 는 쓰지 않는다", outcome).isNull();
    }

    private double total() {
        return meterRegistry.find(METRIC).counters().stream().mapToDouble(Counter::count).sum();
    }

    private void assertCountedOnce(String outcome, double before, double totalBefore) {
        assertThat(count(outcome) - before).as("outcome=%s 증가량", outcome).isEqualTo(1.0);
        assertThat(total() - totalBefore).as("전체 증가량 (다른 태그는 증가하지 않아야 함)").isEqualTo(1.0);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발급 성공 202 → coupon.issue.outcome{outcome=OK} 1 증가")
    void ok() throws Exception {
        // Given
        long eventId = startedEvent(10);
        double before = count("OK");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isAccepted());
        assertCountedOnce("OK", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발행 실패 후 보상 결과 ALREADY_PERSISTED 로 202 성공한 건은 OK 로 센다")
    void alreadyPersistedCountsAsOk() throws Exception {
        // Given
        long eventId = startedEvent(10);
        publisher.failNext();
        gate.nextCompensation(CompensationResult.ALREADY_PERSISTED);
        double before = count("OK");
        double failedBefore = count("TEMPORARILY_FAILED");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isAccepted());
        assertCountedOnce("OK", before, totalBefore);
        assertThat(count("TEMPORARILY_FAILED")).as("ALREADY_PERSISTED 는 실패로 세지 않는다").isEqualTo(failedBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발급 시작 전 409 → outcome=NOT_STARTED")
    void notStarted() throws Exception {
        // Given
        long eventId = EVENT_IDS.incrementAndGet();
        gate.initialize(eventId, 10, NOW.plus(Duration.ofMinutes(1)));
        double before = count("NOT_STARTED");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("COUPON_NOT_STARTED"));
        assertCountedOnce("NOT_STARTED", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 이미 발급 409 → outcome=DUPLICATE")
    void duplicate() throws Exception {
        // Given
        long eventId = startedEvent(10);
        gate.givenIssued(eventId, 1L, "r-prev");
        double before = count("DUPLICATE");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("COUPON_DUPLICATE"));
        assertCountedOnce("DUPLICATE", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 수량 소진 409 COUPON_SOLD_OUT → outcome=SOLD_OUT")
    void soldOut() throws Exception {
        // Given
        long eventId = startedEvent(0);
        double before = count("SOLD_OUT");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("COUPON_SOLD_OUT"));
        assertCountedOnce("SOLD_OUT", before, totalBefore);
        assertNoTag("COUPON_SOLD_OUT");
    }

    @Test
    @DisplayName("[8단계 메트릭] Redis meta 없음 404 COUPON_EVENT_NOT_FOUND → outcome=NOT_FOUND (IssueOutcome 상수명, ErrorCode 이름 EVENT_NOT_FOUND 아님)")
    void notFound() throws Exception {
        // Given
        long eventId = EVENT_IDS.incrementAndGet();     // initialize 하지 않음
        double before = count("NOT_FOUND");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isNotFound()).andExpect(jsonPath("$.code").value("COUPON_EVENT_NOT_FOUND"));
        assertCountedOnce("NOT_FOUND", before, totalBefore);
        assertNoTag("EVENT_NOT_FOUND");
    }

    @Test
    @DisplayName("[8단계 메트릭] Redis 게이트 장애 503 → outcome=TEMPORARILY_FAILED")
    void gateUnavailable() throws Exception {
        // Given
        long eventId = startedEvent(10);
        gate.timeoutBeforeIssueNext();
        double before = count("TEMPORARILY_FAILED");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("COUPON_TEMPORARILY_FAILED"));
        assertCountedOnce("TEMPORARILY_FAILED", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발행 실패 후 보상 COMPENSATED 로 503 → outcome=TEMPORARILY_FAILED (OK 로 세지 않는다)")
    void publishFailedCompensated() throws Exception {
        // Given
        long eventId = startedEvent(10);
        publisher.failNext();
        double before = count("TEMPORARILY_FAILED");
        double okBefore = count("OK");
        double totalBefore = total();

        // When
        ResultActions result = issue(eventId, 1L);

        // Then
        result.andExpect(status().isServiceUnavailable());
        assertCountedOnce("TEMPORARILY_FAILED", before, totalBefore);
        assertThat(count("OK")).isEqualTo(okBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 서비스 호출 전 실패(X-User-Id 누락 400)는 판정이 아니므로 세지 않는다")
    void missingHeaderNotCounted() throws Exception {
        // Given
        double totalBefore = total();

        // When
        ResultActions result = mockMvc.perform(post("/api/coupons/events/{eventId}/issue", 1L));

        // Then
        result.andExpect(status().isBadRequest());
        assertThat(total()).isEqualTo(totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발급이 아닌 API 의 에러(내 쿠폰 조회 실패)는 coupon.issue.outcome 에 세지 않는다")
    void otherEndpointErrorNotCounted() throws Exception {
        // Given
        willThrow(new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED))
                .given(queryService).myCoupons(anyLong());
        double totalBefore = total();

        // When
        ResultActions result = mockMvc.perform(get("/api/coupons/me").header("X-User-Id", 1L));

        // Then
        result.andExpect(status().isServiceUnavailable());
        assertThat(total()).isEqualTo(totalBefore);
    }
}
