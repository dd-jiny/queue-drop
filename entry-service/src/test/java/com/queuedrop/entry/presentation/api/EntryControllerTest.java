package com.queuedrop.entry.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.dto.MyEntryResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.service.EntryApplyService;
import com.queuedrop.entry.application.service.EntryQueryService;
import java.time.Instant;
import java.util.List;
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
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** presentation 슬라이스. domain 타입(ApplyOutcome, EntryStatus 등)은 import 하지 않는다 — status 는 문자열로만 다룬다. */
@WebMvcTest(EntryController.class)
@Import(GlobalExceptionHandler.class)
class EntryControllerTest {

    private static final String APPLY_PATH = "/api/entries/drops/{dropId}";
    private static final String MY_ENTRIES_PATH = "/api/entries/me";
    private static final String MY_ENTRY_PATH = "/api/entries/drops/{dropId}/me";
    private static final Instant APPLIED_AT = Instant.parse("2026-12-01T01:00:05Z");

    @Autowired MockMvc mockMvc;
    @MockitoBean EntryApplyService entryApplyService;
    @MockitoBean EntryQueryService entryQueryService;

    private ResultActions perform(MockHttpServletRequestBuilder request, String userId) throws Exception {
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        return mockMvc.perform(request);
    }

    private ResultActions apply(String dropId, String userId) throws Exception {
        return perform(post(APPLY_PATH, dropId), userId);
    }

    private ResultActions findMyEntries(String userId) throws Exception {
        return perform(get(MY_ENTRIES_PATH), userId);
    }

    private ResultActions findMyEntry(String dropId, String userId) throws Exception {
        return perform(get(MY_ENTRY_PATH, dropId), userId);
    }

    /** 응모 API 의 EntryErrorCode → HTTP 상태, 코드, 고정 메시지 (단계 문서 3.2 에러 응답 표) */
    static Stream<Arguments> applyErrors() {
        return Stream.of(
                Arguments.of(EntryErrorCode.CLOSED, 409, "ENTRY_CLOSED", "응모 기간이 아닙니다."),
                Arguments.of(EntryErrorCode.NO_TOKEN, 409, "ENTRY_NO_TOKEN", "입장 토큰이 없거나 만료되었습니다."),
                Arguments.of(EntryErrorCode.DUPLICATE, 409, "ENTRY_DUPLICATE", "이미 응모한 드로우입니다."),
                Arguments.of(EntryErrorCode.SOLD_OUT, 409, "ENTRY_SOLD_OUT", "응모 정원이 마감되었습니다."),
                Arguments.of(EntryErrorCode.TEMPORARILY_FAILED, 503, "ENTRY_TEMPORARILY_FAILED",
                        "일시적인 오류입니다. 다시 시도해 주세요."));
    }

    @Nested
    @DisplayName("POST /api/entries/drops/{dropId} — 응모")
    class Apply {

        @Test
        @DisplayName("[T4-25] 판정 성공이면 202 와 {\"entrySeq\":7}, Command 에 dropId·userId 가 담긴다")
        void accepted() throws Exception {
            // Given
            given(entryApplyService.apply(any())).willReturn(new EntryApplyResult(7));

            // When
            ResultActions result = apply("10", "3");

            // Then
            result.andExpect(status().isAccepted())
                    .andExpect(content().json("""
                            {"entrySeq":7}""", JsonCompareMode.STRICT));
            ArgumentCaptor<EntryApplyCommand> captor = ArgumentCaptor.forClass(EntryApplyCommand.class);
            verify(entryApplyService).apply(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new EntryApplyCommand(10L, 3L));
        }

        @ParameterizedTest(name = "{0} → {1} {2}")
        @MethodSource("com.queuedrop.entry.presentation.api.EntryControllerTest#applyErrors")
        @DisplayName("[T4-25] EntryErrorCode 는 409/503 과 ENTRY_* 코드·고정 메시지로 응답한다")
        void applyErrors(EntryErrorCode errorCode, int httpStatus, String code, String message) throws Exception {
            // Given
            willThrow(new ApplicationException(errorCode)).given(entryApplyService).apply(any());

            // When
            ResultActions result = apply("10", "3");

            // Then
            result.andExpect(status().is(httpStatus))
                    .andExpect(jsonPath("$.code").value(code))
                    .andExpect(jsonPath("$.message").value(message));
        }

        @Test
        @DisplayName("[T4-25] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = apply("10", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(entryApplyService);
        }

        @ParameterizedTest(name = "X-User-Id={0}")
        @ValueSource(strings = {"abc", "1.5"})
        @DisplayName("[T4-25] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader(String userId) throws Exception {
            // When
            ResultActions result = apply("10", userId);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(entryApplyService);
        }

        @Test
        @DisplayName("[T4-25] 숫자가 아닌 dropId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
        void nonNumericDropId() throws Exception {
            // When
            ResultActions result = apply("abc", "3");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(entryApplyService);
        }
    }

    @Nested
    @DisplayName("GET /api/entries/me — 내 응모 목록")
    class MyEntries {

        @Test
        @DisplayName("[T4-25] 200 과 dropId·entrySeq·status·appliedAt 배열, 서비스에 userId 가 전달된다")
        void list() throws Exception {
            // Given
            given(entryQueryService.myEntries(3L)).willReturn(List.of(
                    new MyEntryResult(20L, 5, "WON", APPLIED_AT),
                    new MyEntryResult(10L, 7, "APPLIED", APPLIED_AT)));

            // When
            ResultActions result = findMyEntries("3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.length()").value(2))
                    .andExpect(jsonPath("$[0].dropId").value(20))
                    .andExpect(jsonPath("$[0].entrySeq").value(5))
                    .andExpect(jsonPath("$[0].status").value("WON"))
                    .andExpect(jsonPath("$[0].appliedAt").value("2026-12-01T01:00:05Z"))
                    .andExpect(jsonPath("$[1].dropId").value(10))
                    .andExpect(jsonPath("$[1].status").value("APPLIED"));
            verify(entryQueryService).myEntries(3L);
        }

        @Test
        @DisplayName("[T4-25] 응모 내역이 없으면 200 과 빈 배열")
        void empty() throws Exception {
            // Given
            given(entryQueryService.myEntries(anyLong())).willReturn(List.of());

            // When
            ResultActions result = findMyEntries("3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("[]", JsonCompareMode.STRICT));
        }

        @Test
        @DisplayName("[T4-25] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = findMyEntries(null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(entryQueryService);
        }

        @Test
        @DisplayName("[T4-25] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader() throws Exception {
            // When
            ResultActions result = findMyEntries("abc");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(entryQueryService);
        }
    }

    @Nested
    @DisplayName("GET /api/entries/drops/{dropId}/me — 특정 드로우 내 응모")
    class MyEntry {

        @Test
        @DisplayName("[T4-25] DB 에 있으면 200 과 dropId·entrySeq·status·appliedAt, 서비스에 dropId·userId 가 전달된다")
        void found() throws Exception {
            // Given
            given(entryQueryService.myEntry(10L, 3L)).willReturn(new MyEntryResult(10L, 7, "LOST", APPLIED_AT));

            // When
            ResultActions result = findMyEntry("10", "3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"dropId":10,"entrySeq":7,"status":"LOST","appliedAt":"2026-12-01T01:00:05Z"}""", JsonCompareMode.STRICT));
            verify(entryQueryService).myEntry(10L, 3L);
        }

        @Test
        @DisplayName("[T4-25] Redis 보완(저장 대기) 응답은 200 APPLIED, entrySeq·appliedAt 키가 null 값으로 존재한다")
        void pendingSave() throws Exception {
            // Given
            given(entryQueryService.myEntry(10L, 3L)).willReturn(new MyEntryResult(10L, null, "APPLIED", null));

            // When
            ResultActions result = findMyEntry("10", "3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"dropId":10,"entrySeq":null,"status":"APPLIED","appliedAt":null}""", JsonCompareMode.STRICT));
        }

        @Test
        @DisplayName("[T4-25] 응모 내역이 없으면 404 ENTRY_NOT_FOUND 와 고정 메시지")
        void notFound() throws Exception {
            // Given
            willThrow(new ApplicationException(EntryErrorCode.NOT_FOUND))
                    .given(entryQueryService).myEntry(anyLong(), anyLong());

            // When
            ResultActions result = findMyEntry("10", "3");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("ENTRY_NOT_FOUND"))
                    .andExpect(jsonPath("$.message").value("응모 내역이 없습니다."));
        }

        @Test
        @DisplayName("[T4-25] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = findMyEntry("10", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(entryQueryService);
        }

        @Test
        @DisplayName("[T4-25] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader() throws Exception {
            // When
            ResultActions result = findMyEntry("10", "abc");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(entryQueryService);
        }

        @Test
        @DisplayName("[T4-25] 숫자가 아닌 dropId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
        void nonNumericDropId() throws Exception {
            // When
            ResultActions result = findMyEntry("abc", "3");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(entryQueryService);
        }
    }
}
