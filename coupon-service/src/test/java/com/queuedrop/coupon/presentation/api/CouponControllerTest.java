package com.queuedrop.coupon.presentation.api;

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
import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.dto.CouponIssueResult;
import com.queuedrop.coupon.application.dto.MyCouponResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.service.CouponIssueService;
import com.queuedrop.coupon.application.service.CouponQueryService;
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

/** 발급·내 쿠폰 API 슬라이스 (단계 문서 3.4, T6-18). domain 타입(IssueOutcome 등)은 import 하지 않는다. */
@WebMvcTest(CouponController.class)
@Import(GlobalExceptionHandler.class)
class CouponControllerTest {

    private static final String ISSUE_PATH = "/api/coupons/events/{eventId}/issue";
    private static final String ME_PATH = "/api/coupons/me";
    private static final Instant ISSUED_AT = Instant.parse("2026-12-01T01:00:05Z");

    @Autowired MockMvc mockMvc;
    @MockitoBean CouponIssueService issueService;
    @MockitoBean CouponQueryService queryService;

    private ResultActions perform(MockHttpServletRequestBuilder request, String userId) throws Exception {
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        return mockMvc.perform(request);
    }

    private ResultActions issue(String eventId, String userId) throws Exception {
        return perform(post(ISSUE_PATH, eventId), userId);
    }

    private ResultActions me(String userId) throws Exception {
        return perform(get(ME_PATH), userId);
    }

    /** 발급 에러 코드 → HTTP 상태·코드·고정 메시지 */
    static Stream<Arguments> issueErrors() {
        return Stream.of(
                Arguments.of(CouponErrorCode.NOT_STARTED, 409, "COUPON_NOT_STARTED"),
                Arguments.of(CouponErrorCode.DUPLICATE, 409, "COUPON_DUPLICATE"),
                Arguments.of(CouponErrorCode.SOLD_OUT, 409, "COUPON_SOLD_OUT"),
                Arguments.of(CouponErrorCode.EVENT_NOT_FOUND, 404, "COUPON_EVENT_NOT_FOUND"),
                Arguments.of(CouponErrorCode.TEMPORARILY_FAILED, 503, "COUPON_TEMPORARILY_FAILED"));
    }

    @Nested
    @DisplayName("POST /api/coupons/events/{eventId}/issue — 발급")
    class Issue {

        @Test
        @DisplayName("[T6-18] 발급 성공이면 202 와 {couponEventId, issuedAt}, Command 에 eventId·userId 가 담긴다")
        void accepted() throws Exception {
            // Given
            given(issueService.issue(any())).willReturn(new CouponIssueResult(10L, ISSUED_AT));

            // When
            ResultActions result = issue("10", "3");

            // Then
            result.andExpect(status().isAccepted())
                    .andExpect(content().json("""
                            {"couponEventId":10,"issuedAt":"2026-12-01T01:00:05Z"}""", JsonCompareMode.STRICT));
            ArgumentCaptor<CouponIssueCommand> captor = ArgumentCaptor.forClass(CouponIssueCommand.class);
            verify(issueService).issue(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new CouponIssueCommand(10L, 3L));
        }

        @ParameterizedTest(name = "{0} → {1} {2}")
        @MethodSource("com.queuedrop.coupon.presentation.api.CouponControllerTest#issueErrors")
        @DisplayName("[T6-18] 발급 에러는 409/404/503 과 COUPON_* 코드·고정 메시지로 응답한다")
        void issueErrors(CouponErrorCode errorCode, int httpStatus, String code) throws Exception {
            // Given
            willThrow(new ApplicationException(errorCode)).given(issueService).issue(any());

            // When
            ResultActions result = issue("10", "3");

            // Then
            result.andExpect(status().is(httpStatus))
                    .andExpect(jsonPath("$.code").value(code))
                    .andExpect(jsonPath("$.message").value(errorCode.message()));
        }

        @Test
        @DisplayName("[T6-18] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = issue("10", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(issueService);
        }

        @ParameterizedTest(name = "X-User-Id={0}")
        @ValueSource(strings = {"abc", "1.5"})
        @DisplayName("[T6-18 보강] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader(String userId) throws Exception {
            // When
            ResultActions result = issue("10", userId);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(issueService);
        }

        @ParameterizedTest(name = "eventId={0}")
        @ValueSource(strings = {"abc", "1.5", "-1"})
        @DisplayName("[T6-18] 숫자가 아닌 eventId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
        void nonNumericEventId(String eventId) throws Exception {
            // When
            ResultActions result = issue(eventId, "3");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(issueService);
        }

        @Test
        @DisplayName("[T6-18 보강] GET 으로 발급 경로를 부르면 405 COMMON_METHOD_NOT_ALLOWED")
        void getNotAllowed() throws Exception {
            // When
            ResultActions result = perform(get(ISSUE_PATH, "10"), "3");

            // Then
            result.andExpect(status().isMethodNotAllowed())
                    .andExpect(jsonPath("$.code").value("COMMON_METHOD_NOT_ALLOWED"));
            verifyNoInteractions(issueService);
        }
    }

    @Nested
    @DisplayName("GET /api/coupons/me — 내 쿠폰 목록")
    class Me {

        @Test
        @DisplayName("[T6-18] 200 과 couponEventId·eventName·issuedAt 배열을 서비스 순서 그대로, 서비스에 userId 가 전달된다")
        void list() throws Exception {
            // Given
            given(queryService.myCoupons(3L)).willReturn(List.of(
                    new MyCouponResult(20L, "여름 쿠폰", ISSUED_AT.plusSeconds(60)),
                    new MyCouponResult(10L, "봄 쿠폰", ISSUED_AT)));

            // When
            ResultActions result = me("3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            [{"couponEventId":20,"eventName":"여름 쿠폰","issuedAt":"2026-12-01T01:01:05Z"},
                             {"couponEventId":10,"eventName":"봄 쿠폰","issuedAt":"2026-12-01T01:00:05Z"}]""",
                            JsonCompareMode.STRICT));
            verify(queryService).myCoupons(3L);
        }

        @Test
        @DisplayName("[T6-18] 쿠폰이 없으면 200 과 빈 배열")
        void empty() throws Exception {
            // Given
            given(queryService.myCoupons(anyLong())).willReturn(List.of());

            // When
            ResultActions result = me("3");

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("[]", JsonCompareMode.STRICT));
        }

        @Test
        @DisplayName("[T6-18] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = me(null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(queryService);
        }

        @Test
        @DisplayName("[T6-18 보강] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader() throws Exception {
            // When
            ResultActions result = me("abc");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(queryService);
        }
    }
}
