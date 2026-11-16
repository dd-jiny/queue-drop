package com.queuedrop.queue.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.queue.application.dto.QueueEnterCommand;
import com.queuedrop.queue.application.dto.QueueStatusQuery;
import com.queuedrop.queue.application.dto.QueueStatusResult;
import com.queuedrop.queue.application.exception.QueueErrorCode;
import com.queuedrop.queue.application.service.QueueService;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** presentation 슬라이스. domain 타입(QueueState 등)은 import 하지 않는다 — state 는 문자열로만 다룬다. */
@WebMvcTest(QueueController.class)
@Import(GlobalExceptionHandler.class)
class QueueControllerTest {

    private static final String PATH = "/api/queue/drops/{dropId}";

    @Autowired MockMvc mockMvc;
    @MockitoBean QueueService queueService;

    private ResultActions perform(MockHttpServletRequestBuilder request, String userId) throws Exception {
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        return mockMvc.perform(request);
    }

    private ResultActions enter(String dropId, String userId) throws Exception {
        return perform(post(PATH, dropId), userId);
    }

    private ResultActions queryStatus(String dropId, String userId) throws Exception {
        return perform(get(PATH, dropId), userId);
    }

    /** QueueErrorCode → HTTP 상태, 코드, 고정 메시지 (단계 문서 3.2 에러 코드 표) */
    static Stream<Arguments> queueErrors() {
        return Stream.of(
                Arguments.of(QueueErrorCode.DROP_NOT_FOUND, 404, "QUEUE_DROP_NOT_FOUND", "대기열 정보가 없는 드로우입니다."),
                Arguments.of(QueueErrorCode.NOT_OPEN, 409, "QUEUE_NOT_OPEN", "아직 오픈 전입니다."),
                Arguments.of(QueueErrorCode.CLOSED, 409, "QUEUE_CLOSED", "응모가 마감되었습니다."),
                Arguments.of(QueueErrorCode.SOLD_OUT, 409, "QUEUE_SOLD_OUT", "응모 정원이 모두 찼습니다."));
    }

    @Nested
    @DisplayName("POST /api/queue/drops/{dropId} — 진입")
    class Enter {

        @Test
        @DisplayName("[T3-23] 대기 진입이면 200 과 state·aheadCount·nextPollAfterMs, Command 에 dropId·userId 가 담긴다")
        void waiting() throws Exception {
            // Given
            given(queueService.enter(any())).willReturn(new QueueStatusResult("WAITING", 1523L, 5000));

            // When
            ResultActions result = enter("10", "7");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.state").value("WAITING"))
                    .andExpect(jsonPath("$.aheadCount").value(1523))
                    .andExpect(jsonPath("$.nextPollAfterMs").value(5000));
            ArgumentCaptor<QueueEnterCommand> captor = ArgumentCaptor.forClass(QueueEnterCommand.class);
            verify(queueService).enter(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new QueueEnterCommand(10L, 7L));
        }

        @Test
        @DisplayName("[T3-23] 이미 입장이면 200 ADMITTED, aheadCount 는 null 로 내려가고 nextPollAfterMs 0")
        void admitted() throws Exception {
            // Given
            given(queueService.enter(any())).willReturn(new QueueStatusResult("ADMITTED", null, 0));

            // When
            ResultActions result = enter("10", "7");

            // Then — 응답 예시(단계 문서 3.4)처럼 aheadCount 키가 null 값으로 존재
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"state":"ADMITTED","aheadCount":null,"nextPollAfterMs":0}"""));
        }

        @ParameterizedTest(name = "{0} → {1} {2}")
        @MethodSource("com.queuedrop.queue.presentation.api.QueueControllerTest#queueErrors")
        @DisplayName("[T3-23] QueueErrorCode 는 404/409 와 QUEUE_* 코드·고정 메시지로 응답한다")
        void queueErrors(QueueErrorCode errorCode, int httpStatus, String code, String message) throws Exception {
            // Given
            willThrow(new ApplicationException(errorCode)).given(queueService).enter(any());

            // When
            ResultActions result = enter("10", "7");

            // Then
            result.andExpect(status().is(httpStatus))
                    .andExpect(jsonPath("$.code").value(code))
                    .andExpect(jsonPath("$.message").value(message));
        }

        @Test
        @DisplayName("[T3-23] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = enter("10", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(queueService);
        }

        @ParameterizedTest(name = "X-User-Id={0}")
        @ValueSource(strings = {"abc", "1.5"})
        @DisplayName("[T3-23] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader(String userId) throws Exception {
            // When
            ResultActions result = enter("10", userId);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(queueService);
        }

        @Test
        @DisplayName("[T3-23] 숫자가 아닌 dropId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
        void nonNumericDropId() throws Exception {
            // When
            ResultActions result = enter("abc", "7");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(queueService);
        }
    }

    @Nested
    @DisplayName("GET /api/queue/drops/{dropId} — 상태 조회")
    class Status {

        @Test
        @DisplayName("[T3-23] 대기 중이면 200 WAITING 과 aheadCount·nextPollAfterMs, Query 에 dropId·userId 가 담긴다")
        void waiting() throws Exception {
            // Given
            given(queueService.status(any())).willReturn(new QueueStatusResult("WAITING", 42L, 1000));

            // When
            ResultActions result = queryStatus("10", "7");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.state").value("WAITING"))
                    .andExpect(jsonPath("$.aheadCount").value(42))
                    .andExpect(jsonPath("$.nextPollAfterMs").value(1000));
            ArgumentCaptor<QueueStatusQuery> captor = ArgumentCaptor.forClass(QueueStatusQuery.class);
            verify(queueService).status(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new QueueStatusQuery(10L, 7L));
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = {"ADMITTED", "SOLD_OUT", "NOT_IN_QUEUE"})
        @DisplayName("[T3-23] 대기가 아닌 상태는 200 과 aheadCount null, nextPollAfterMs 0")
        void nonWaitingStates(String state) throws Exception {
            // Given
            given(queueService.status(any())).willReturn(new QueueStatusResult(state, null, 0));

            // When
            ResultActions result = queryStatus("10", "7");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"state":"%s","aheadCount":null,"nextPollAfterMs":0}""".formatted(state)));
        }

        @Test
        @DisplayName("[T3-23] meta 가 없으면 404 QUEUE_DROP_NOT_FOUND")
        void dropNotFound() throws Exception {
            // Given
            willThrow(new ApplicationException(QueueErrorCode.DROP_NOT_FOUND)).given(queueService).status(any());

            // When
            ResultActions result = queryStatus("10", "7");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("QUEUE_DROP_NOT_FOUND"))
                    .andExpect(jsonPath("$.message").value("대기열 정보가 없는 드로우입니다."));
        }

        @Test
        @DisplayName("[T3-23] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = queryStatus("10", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(queueService);
        }

        @Test
        @DisplayName("[T3-23] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT")
        void invalidUserHeader() throws Exception {
            // When
            ResultActions result = queryStatus("10", "abc");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(queueService);
        }

        @Test
        @DisplayName("[T3-23] 숫자가 아닌 dropId 는 404 COMMON_NOT_FOUND")
        void nonNumericDropId() throws Exception {
            // When
            ResultActions result = queryStatus("abc", "7");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(queueService);
        }
    }
}
