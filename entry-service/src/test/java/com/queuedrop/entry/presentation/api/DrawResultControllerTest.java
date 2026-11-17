package com.queuedrop.entry.presentation.api;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.application.service.DrawQueryService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/** 추첨 결과 조회 슬라이스 (단계 문서 5단계 3.4, T5-23). 공개 API 라 X-User-* 헤더 없이 호출한다. domain 타입 import 금지 */
@WebMvcTest(DrawResultController.class)
@Import({GlobalExceptionHandler.class, SimpleMeterRegistry.class})
class DrawResultControllerTest {

    private static final String RESULT_PATH = "/api/entries/drops/{dropId}/result";
    private static final Instant DRAWN_AT = Instant.parse("2026-11-20T12:00:05Z");
    /** 2^53 + 1 — JavaScript Number 로는 정확히 표현되지 않는 값 */
    private static final long UNSAFE_SEED = 9_007_199_254_740_993L;

    @Autowired MockMvc mockMvc;
    @MockitoBean DrawQueryService drawQueryService;

    private ResultActions findResult(String dropId) throws Exception {
        return mockMvc.perform(get(RESULT_PATH, dropId));
    }

    @Test
    @DisplayName("[T5-23] 추첨 전이면 409 ENTRY_DRAW_NOT_COMPLETED 와 고정 메시지")
    void notCompleted() throws Exception {
        // Given
        willThrow(new ApplicationException(DrawErrorCode.NOT_COMPLETED)).given(drawQueryService).summary(anyLong());

        // When
        ResultActions result = findResult("10");

        // Then
        result.andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ENTRY_DRAW_NOT_COMPLETED"))
                .andExpect(jsonPath("$.message").value("아직 추첨이 완료되지 않았습니다."));
    }

    @Test
    @DisplayName("[T5-23] 추첨 후면 헤더 없이도 200, seed 는 문자열(2^53 초과 값도 정확히), dropId 가 서비스에 전달된다")
    void completed() throws Exception {
        // Given
        given(drawQueryService.summary(10L)).willReturn(new DrawSummaryResult(10L, 100, 3, UNSAFE_SEED, DRAWN_AT));

        // When
        ResultActions result = findResult("10");

        // Then
        result.andExpect(status().isOk())
                .andExpect(content().json("""
                        {"dropId":10,"applicantCount":100,"winnerCount":3,"seed":"9007199254740993",
                         "drawnAt":"2026-11-20T12:00:05Z"}""", JsonCompareMode.STRICT));
        verify(drawQueryService).summary(10L);
    }

    @Test
    @DisplayName("[T5-23] 음수 seed 도 부호 포함 문자열로 내보낸다")
    void negativeSeed() throws Exception {
        // Given
        given(drawQueryService.summary(10L)).willReturn(new DrawSummaryResult(10L, 0, 0, Long.MIN_VALUE, DRAWN_AT));

        // When
        ResultActions result = findResult("10");

        // Then
        result.andExpect(status().isOk())
                .andExpect(jsonPath("$.seed").value("-9223372036854775808"))
                .andExpect(jsonPath("$.applicantCount").value(0))
                .andExpect(jsonPath("$.winnerCount").value(0));
    }

    @ParameterizedTest(name = "seed={0}")
    @ValueSource(longs = {0L, -1L, Long.MAX_VALUE})
    @DisplayName("[T5-23] seed 0·-1·Long.MAX_VALUE 도 Long.toString 그대로의 문자열로 내보낸다")
    void seedBoundaries(long seed) throws Exception {
        // Given
        given(drawQueryService.summary(10L)).willReturn(new DrawSummaryResult(10L, 1, 1, seed, DRAWN_AT));

        // When
        ResultActions result = findResult("10");

        // Then
        result.andExpect(status().isOk())
                .andExpect(jsonPath("$.seed").value(Long.toString(seed)));
    }

    @ParameterizedTest(name = "dropId={0}")
    @ValueSource(strings = {"abc", "1.5", "-1"})
    @DisplayName("[T5-23] 숫자가 아닌 dropId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
    void nonNumericDropId(String dropId) throws Exception {
        // When
        ResultActions result = findResult(dropId);

        // Then
        result.andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
        verifyNoInteractions(drawQueryService);
    }
}
