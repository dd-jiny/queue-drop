package com.queuedrop.entry.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.willThrow;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.service.EntryApplyService;
import com.queuedrop.entry.application.service.EntryQueryService;
import com.queuedrop.entry.application.service.fake.FakeEntryEventPublisher;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
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
 * 응모 판정 결과 카운터 entry.apply.outcome{outcome} (단계 문서 8단계 3.2 "관찰할 메트릭").
 * 태그 값은 ErrorCode 접두사 없는 판정 이름(OK/CLOSED/NO_TOKEN/DUPLICATE/SOLD_OUT/TEMPORARILY_FAILED).
 * 기록 위치는 presentation — 실제 EntryApplyService 에 Fake 포트를 붙여 응답 경로 전체로 검증한다.
 * domain 타입(ApplyOutcome)은 import 하지 않는다.
 */
@WebMvcTest(EntryController.class)
@Import({GlobalExceptionHandler.class, EntryApplyMetricsTest.Config.class})
class EntryApplyMetricsTest {

    private static final String METRIC = "entry.apply.outcome";
    private static final Instant NOW = Instant.parse("2026-12-01T01:00:00Z");
    /** 테스트마다 다른 드로우를 써서 공유 Fake 상태가 섞이지 않게 한다 */
    private static final AtomicLong DROP_IDS = new AtomicLong(1000);

    @TestConfiguration
    static class Config {
        @Bean FakeEntryGate fakeEntryGate() { return new FakeEntryGate(); }
        @Bean FakeEntryEventPublisher fakeEntryEventPublisher() { return new FakeEntryEventPublisher(); }
        @Bean Clock clock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
        @Bean EntryApplyService entryApplyService(FakeEntryGate gate, FakeEntryEventPublisher publisher, Clock clock) {
            return new EntryApplyService(gate, publisher, clock);
        }
    }

    @Autowired MockMvc mockMvc;
    @Autowired MeterRegistry meterRegistry;
    @Autowired FakeEntryGate gate;
    @Autowired FakeEntryEventPublisher publisher;
    @MockitoBean EntryQueryService entryQueryService;

    // ---- 도우미 ----
    private long openDrop(int capacity) {
        long dropId = DROP_IDS.incrementAndGet();
        gate.initialize(dropId, capacity, NOW.minus(Duration.ofMinutes(10)), NOW.plus(Duration.ofMinutes(10)));
        return dropId;
    }

    private ResultActions apply(long dropId, long userId) throws Exception {
        return mockMvc.perform(post("/api/entries/drops/{dropId}", dropId).header("X-User-Id", userId));
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

    /** 한 요청이 지정한 outcome 하나만 1 올렸는지 */
    private void assertCountedOnce(String outcome, double before, double totalBefore) {
        assertThat(count(outcome) - before).as("outcome=%s 증가량", outcome).isEqualTo(1.0);
        assertThat(total() - totalBefore).as("전체 증가량 (다른 태그는 증가하지 않아야 함)").isEqualTo(1.0);
    }

    @Test
    @DisplayName("[8단계 메트릭] 판정 성공 202 → entry.apply.outcome{outcome=OK} 1 증가")
    void ok() throws Exception {
        // Given
        long dropId = openDrop(10);
        gate.grantToken(dropId, 1L);
        double before = count("OK");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isAccepted());
        assertCountedOnce("OK", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발행 실패 후 보상 결과 ALREADY_PERSISTED 로 202 성공한 건은 OK 로 센다")
    void alreadyPersistedCountsAsOk() throws Exception {
        // Given
        long dropId = DROP_IDS.incrementAndGet();
        gate.nextOutcome(GateResult.ok(1));
        publisher.failNext();
        gate.nextCompensation(CompensationResult.ALREADY_PERSISTED);
        double before = count("OK");
        double failedBefore = count("TEMPORARILY_FAILED");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isAccepted());
        assertCountedOnce("OK", before, totalBefore);
        assertThat(count("TEMPORARILY_FAILED")).as("ALREADY_PERSISTED 는 실패로 세지 않는다").isEqualTo(failedBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 초기화 전(마감) 드로우 409 ENTRY_CLOSED → outcome=CLOSED (응답 code 가 아니라 판정 이름)")
    void closed() throws Exception {
        // Given
        long dropId = DROP_IDS.incrementAndGet();      // initialize 하지 않음 → CLOSED
        double before = count("CLOSED");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("ENTRY_CLOSED"));
        assertCountedOnce("CLOSED", before, totalBefore);
        assertNoTag("ENTRY_CLOSED");
    }

    @Test
    @DisplayName("[8단계 메트릭] 토큰 없음 409 → outcome=NO_TOKEN")
    void noToken() throws Exception {
        // Given
        long dropId = openDrop(10);
        double before = count("NO_TOKEN");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("ENTRY_NO_TOKEN"));
        assertCountedOnce("NO_TOKEN", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 이미 응모 409 → outcome=DUPLICATE")
    void duplicate() throws Exception {
        // Given
        long dropId = openDrop(10);
        gate.grantToken(dropId, 1L);
        gate.givenApplicant(dropId, 1L);
        double before = count("DUPLICATE");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("ENTRY_DUPLICATE"));
        assertCountedOnce("DUPLICATE", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 정원 소진 409 ENTRY_SOLD_OUT → outcome=SOLD_OUT")
    void soldOut() throws Exception {
        // Given
        long dropId = openDrop(0);
        gate.grantToken(dropId, 1L);
        double before = count("SOLD_OUT");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("ENTRY_SOLD_OUT"));
        assertCountedOnce("SOLD_OUT", before, totalBefore);
        assertNoTag("ENTRY_SOLD_OUT");
    }

    @Test
    @DisplayName("[8단계 메트릭] Redis 게이트 장애 503 → outcome=TEMPORARILY_FAILED")
    void gateUnavailable() throws Exception {
        // Given
        long dropId = openDrop(10);
        gate.grantToken(dropId, 1L);
        gate.timeoutBeforeApplyNext();
        double before = count("TEMPORARILY_FAILED");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

        // Then
        result.andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("ENTRY_TEMPORARILY_FAILED"));
        assertCountedOnce("TEMPORARILY_FAILED", before, totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 발행 실패 후 보상 COMPENSATED 로 503 → outcome=TEMPORARILY_FAILED (OK 로 세지 않는다)")
    void publishFailedCompensated() throws Exception {
        // Given
        long dropId = openDrop(10);
        gate.grantToken(dropId, 1L);
        publisher.failNext();
        double before = count("TEMPORARILY_FAILED");
        double okBefore = count("OK");
        double totalBefore = total();

        // When
        ResultActions result = apply(dropId, 1L);

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
        ResultActions result = mockMvc.perform(post("/api/entries/drops/{dropId}", 1L));

        // Then
        result.andExpect(status().isBadRequest());
        assertThat(total()).isEqualTo(totalBefore);
    }

    @Test
    @DisplayName("[8단계 메트릭] 응모가 아닌 API 의 에러(내 응모 단건 조회 404 ENTRY_NOT_FOUND)는 entry.apply.outcome 에 세지 않는다")
    void otherEndpointErrorNotCounted() throws Exception {
        // Given
        willThrow(new ApplicationException(EntryErrorCode.NOT_FOUND))
                .given(entryQueryService).myEntry(anyLong(), anyLong());
        double totalBefore = total();

        // When
        ResultActions result = mockMvc.perform(get("/api/entries/drops/{dropId}/me", 1L).header("X-User-Id", 1L));

        // Then
        result.andExpect(status().isNotFound());
        assertThat(total()).isEqualTo(totalBefore);
    }
}
