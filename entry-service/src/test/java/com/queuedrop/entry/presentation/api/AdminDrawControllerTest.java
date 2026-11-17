package com.queuedrop.entry.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.dto.ManualDrawCommand;
import com.queuedrop.entry.application.dto.Requester;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.application.service.DrawService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** 관리자 수동 추첨 슬라이스 (단계 문서 5단계 3.4 D5-1, T5-32). domain 타입 import 금지 */
@WebMvcTest(AdminDrawController.class)
@Import({GlobalExceptionHandler.class, AdminDrawControllerTest.FixedClockConfig.class, SimpleMeterRegistry.class})
class AdminDrawControllerTest {

    private static final String DRAW_PATH = "/api/entries/admin/drops/{dropId}/draw";
    private static final Instant NOW = Instant.parse("2026-11-20T12:10:00Z");
    private static final Instant DRAWN_AT = NOW;
    private static final long UNSAFE_SEED = 9_007_199_254_740_993L;   // 2^53 + 1

    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {
        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }

    @Autowired MockMvc mockMvc;
    @MockitoBean DrawService drawService;

    private ResultActions draw(String dropId, String userId, String role) throws Exception {
        MockHttpServletRequestBuilder request = post(DRAW_PATH, dropId);
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        if (role != null) {
            request.header("X-User-Role", role);
        }
        return mockMvc.perform(request);
    }

    /** drawManually 가 던지는 에러 → HTTP 매핑 (3.4 API 표: 403 / 404 / 409) */
    static Stream<Arguments> drawErrors() {
        return Stream.of(
                Arguments.of(CommonErrorCode.FORBIDDEN, 403, "COMMON_FORBIDDEN"),
                Arguments.of(DrawErrorCode.DROP_NOT_FOUND, 404, "ENTRY_DRAW_DROP_NOT_FOUND"),
                Arguments.of(DrawErrorCode.NOT_CLOSING, 409, "ENTRY_DRAW_NOT_CLOSING"),
                Arguments.of(DrawErrorCode.ALREADY_DRAWN, 409, "ENTRY_DRAW_ALREADY_DRAWN"));
    }

    @Test
    @DisplayName("[T5-32] ADMIN 헤더면 200 + seed 문자열, Command 에 Requester(userId, role)·dropId 와 Clock 시각이 전달된다")
    void adminDraws() throws Exception {
        // Given
        given(drawService.drawManually(any(), any())).willReturn(new DrawSummaryResult(1L, 9, 3, UNSAFE_SEED, DRAWN_AT));

        // When
        ResultActions result = draw("1", "7", "ADMIN");

        // Then
        result.andExpect(status().isOk())
                .andExpect(content().json("""
                        {"dropId":1,"applicantCount":9,"winnerCount":3,"seed":"9007199254740993",
                         "drawnAt":"2026-11-20T12:10:00Z"}""", JsonCompareMode.STRICT));
        ArgumentCaptor<ManualDrawCommand> command = ArgumentCaptor.forClass(ManualDrawCommand.class);
        ArgumentCaptor<Instant> now = ArgumentCaptor.forClass(Instant.class);
        verify(drawService).drawManually(command.capture(), now.capture());
        assertThat(command.getValue()).isEqualTo(new ManualDrawCommand(new Requester(7L, "ADMIN"), 1L));
        assertThat(now.getValue()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("[T5-32] 헤더가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
    void missingHeaders() throws Exception {
        // When
        ResultActions result = draw("1", null, null);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
        verifyNoInteractions(drawService);
    }

    @Test
    @DisplayName("[T5-32] X-User-Role 만 없어도 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
    void missingRoleHeader() throws Exception {
        // When
        ResultActions result = draw("1", "7", null);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
        verifyNoInteractions(drawService);
    }

    @Test
    @DisplayName("[T5-32] USER 역할도 컨트롤러는 그대로 전달한다 (권한 판단은 application 의 AdminGuard)")
    void userRolePassedThrough() throws Exception {
        // Given
        willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN)).given(drawService).drawManually(any(), any());

        // When
        ResultActions result = draw("1", "7", "USER");

        // Then
        result.andExpect(status().isForbidden());
        ArgumentCaptor<ManualDrawCommand> command = ArgumentCaptor.forClass(ManualDrawCommand.class);
        verify(drawService).drawManually(command.capture(), any());
        assertThat(command.getValue().requester()).isEqualTo(new Requester(7L, "USER"));
    }

    @ParameterizedTest(name = "{0} → {1} {2}")
    @MethodSource("drawErrors")
    @DisplayName("[T5-32] drawManually 에러는 403/404/409 와 해당 코드로 응답한다")
    void drawErrors(ErrorCode errorCode, int httpStatus, String code) throws Exception {
        // Given
        willThrow(new ApplicationException(errorCode)).given(drawService).drawManually(any(), any());

        // When
        ResultActions result = draw("1", "7", "ADMIN");

        // Then
        result.andExpect(status().is(httpStatus))
                .andExpect(jsonPath("$.code").value(code))
                .andExpect(jsonPath("$.message").value(errorCode.message()));
    }

    @ParameterizedTest(name = "dropId={0}")
    @ValueSource(strings = {"abc", "1.5", "-1"})
    @DisplayName("[T5-32] 숫자가 아닌 dropId 는 ADMIN 헤더가 있어도 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
    void nonNumericDropId(String dropId) throws Exception {
        // When
        ResultActions result = draw(dropId, "7", "ADMIN");

        // Then
        result.andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
        verifyNoInteractions(drawService);
    }

    @Test
    @DisplayName("[T5-32] 성공 응답에서 seed 0 은 \"0\" 문자열로 내보낸다")
    void zeroSeed() throws Exception {
        // Given
        given(drawService.drawManually(any(), any())).willReturn(new DrawSummaryResult(1L, 0, 0, 0L, DRAWN_AT));

        // When
        ResultActions result = draw("1", "7", "ADMIN");

        // Then
        result.andExpect(status().isOk())
                .andExpect(jsonPath("$.seed").value("0"));
    }
}
