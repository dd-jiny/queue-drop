package com.queuedrop.coupon.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
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
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.coupon.application.dto.CouponEventRegisterCommand;
import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.CouponReinitializeCommand;
import com.queuedrop.coupon.application.dto.Requester;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.service.CouponEventService;
import com.queuedrop.coupon.application.service.CouponQueryService;
import java.time.Instant;
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
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * 쿠폰 이벤트 등록·조회·재초기화 API 슬라이스 (단계 문서 3.4, T6-19). domain 타입 import 금지.
 * @Future 는 Bean Validation 기본 ClockProvider(시스템 시각) 기준이라 과거·미래 값은 시스템 시각과 멀리 둔다.
 */
@WebMvcTest(CouponEventController.class)
@Import(GlobalExceptionHandler.class)
class CouponEventControllerTest {

    private static final String EVENTS_PATH = "/api/coupons/events";
    private static final String EVENT_PATH = "/api/coupons/events/{eventId}";
    private static final String INIT_PATH = "/api/coupons/events/{eventId}/initialize";
    private static final Instant FUTURE_START = Instant.parse("2099-01-01T00:00:00Z");

    @Autowired MockMvc mockMvc;
    @MockitoBean CouponEventService eventService;
    @MockitoBean CouponQueryService queryService;

    private static MockHttpServletRequestBuilder withHeaders(MockHttpServletRequestBuilder request,
                                                             String userId, String role) {
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        if (role != null) {
            request.header("X-User-Role", role);
        }
        return request;
    }

    private ResultActions register(String body, String userId, String role) throws Exception {
        return mockMvc.perform(withHeaders(post(EVENTS_PATH), userId, role)
                .contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private ResultActions registerAsAdmin(String body) throws Exception {
        return register(body, "7", "ADMIN");
    }

    private ResultActions reinitialize(String eventId, String userId, String role) throws Exception {
        return mockMvc.perform(withHeaders(post(INIT_PATH, eventId), userId, role));
    }

    private static String body(String name, Object totalQuantity, String issueStartAt) {
        StringBuilder sb = new StringBuilder("{");
        if (name != null) {
            sb.append("\"name\":\"").append(name).append("\",");
        }
        if (totalQuantity != null) {
            sb.append("\"totalQuantity\":").append(totalQuantity).append(',');
        }
        if (issueStartAt != null) {
            sb.append("\"issueStartAt\":\"").append(issueStartAt).append("\",");
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1);
        }
        return sb.append('}').toString();
    }

    /** 필드 하나만 잘못된 본문 → 그 필드가 fieldErrors 에 담긴 400 */
    static Stream<Arguments> invalidBodies() {
        String future = FUTURE_START.toString();
        return Stream.of(
                Arguments.of("수량 0", body("봄 쿠폰", 0, future), "totalQuantity"),
                Arguments.of("수량 음수", body("봄 쿠폰", -1, future), "totalQuantity"),
                Arguments.of("과거 시작 시각", body("봄 쿠폰", 100, "2000-01-01T00:00:00Z"), "issueStartAt"),
                Arguments.of("시작 시각 누락", body("봄 쿠폰", 100, null), "issueStartAt"),
                Arguments.of("빈 이름", body(" ", 100, future), "name"),
                Arguments.of("이름 누락", body(null, 100, future), "name"));
    }

    @Nested
    @DisplayName("POST /api/coupons/events — 등록")
    class Register {

        @Test
        @DisplayName("[T6-19] 정상 요청이면 201 과 {id, name, totalQuantity, remaining, issueStartAt}, Command 에 Requester·본문이 담긴다")
        void created() throws Exception {
            // Given
            given(eventService.register(any())).willReturn(
                    new CouponEventResult(1L, "봄 쿠폰", 100, 100L, FUTURE_START));

            // When
            ResultActions result = registerAsAdmin(body("봄 쿠폰", 100, FUTURE_START.toString()));

            // Then
            result.andExpect(status().isCreated())
                    .andExpect(content().json("""
                            {"id":1,"name":"봄 쿠폰","totalQuantity":100,"remaining":100,
                             "issueStartAt":"2099-01-01T00:00:00Z"}""", JsonCompareMode.STRICT));
            ArgumentCaptor<CouponEventRegisterCommand> captor = ArgumentCaptor.forClass(CouponEventRegisterCommand.class);
            verify(eventService).register(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new CouponEventRegisterCommand(
                    new Requester(7L, "ADMIN"), "봄 쿠폰", 100, FUTURE_START));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.queuedrop.coupon.presentation.api.CouponEventControllerTest#invalidBodies")
        @DisplayName("[T6-19] 요청 검증 실패는 400 COMMON_INVALID_INPUT 과 해당 필드 오류, 서비스는 호출되지 않는다")
        void invalid(String caseName, String body, String field) throws Exception {
            // When
            ResultActions result = registerAsAdmin(body);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                    .andExpect(jsonPath("$.fieldErrors[*].field", hasItem(field)));
            verifyNoInteractions(eventService);
        }

        @Test
        @DisplayName("[T6-19 보강] 수량 누락(null)도 400 COMMON_INVALID_INPUT — 언박싱 NPE 로 500 이 되지 않는다")
        void missingQuantity() throws Exception {
            // When
            ResultActions result = registerAsAdmin(body("봄 쿠폰", null, FUTURE_START.toString()));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(eventService);
        }

        @Test
        @DisplayName("[T6-19 보강] JSON 이 아닌 본문은 400 COMMON_INVALID_INPUT")
        void unreadableBody() throws Exception {
            // When
            ResultActions result = registerAsAdmin("{not-json");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(eventService);
        }

        @Test
        @DisplayName("[T6-19 보강] X-User-Role 이 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingRoleHeader() throws Exception {
            // When
            ResultActions result = register(body("봄 쿠폰", 100, FUTURE_START.toString()), "7", null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(eventService);
        }

        @Test
        @DisplayName("[T6-19 보강] USER 역할도 컨트롤러는 그대로 전달하고, AdminGuard 의 FORBIDDEN 은 403")
        void userRoleForbidden() throws Exception {
            // Given
            willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN)).given(eventService).register(any());

            // When
            ResultActions result = register(body("봄 쿠폰", 100, FUTURE_START.toString()), "7", "USER");

            // Then
            result.andExpect(status().isForbidden())
                    .andExpect(jsonPath("$.code").value("COMMON_FORBIDDEN"));
            ArgumentCaptor<CouponEventRegisterCommand> captor = ArgumentCaptor.forClass(CouponEventRegisterCommand.class);
            verify(eventService).register(captor.capture());
            assertThat(captor.getValue().requester()).isEqualTo(new Requester(7L, "USER"));
        }

        @Test
        @DisplayName("[T6-19 보강] 도메인 검증 실패(INVALID_EVENT)는 400 COUPON_INVALID_EVENT")
        void invalidEvent() throws Exception {
            // Given
            willThrow(new ApplicationException(CouponErrorCode.INVALID_EVENT)).given(eventService).register(any());

            // When
            ResultActions result = registerAsAdmin(body("봄 쿠폰", 100, FUTURE_START.toString()));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COUPON_INVALID_EVENT"));
        }
    }

    @Nested
    @DisplayName("GET /api/coupons/events/{eventId} — 조회 (공개)")
    class Get {

        @Test
        @DisplayName("[T6-19] 헤더 없이 200 과 remaining 을 포함한 응답, 서비스에 eventId 가 전달된다")
        void found() throws Exception {
            // Given
            given(queryService.event(5L)).willReturn(new CouponEventResult(5L, "봄 쿠폰", 100, 37L, FUTURE_START));

            // When
            ResultActions result = mockMvc.perform(get(EVENT_PATH, "5"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"id":5,"name":"봄 쿠폰","totalQuantity":100,"remaining":37,
                             "issueStartAt":"2099-01-01T00:00:00Z"}""", JsonCompareMode.STRICT));
            verify(queryService).event(5L);
        }

        static Stream<Arguments> getErrors() {
            return Stream.of(
                    Arguments.of(CouponErrorCode.EVENT_NOT_FOUND, 404, "COUPON_EVENT_NOT_FOUND"),
                    Arguments.of(CouponErrorCode.TEMPORARILY_FAILED, 503, "COUPON_TEMPORARILY_FAILED"));
        }

        @ParameterizedTest(name = "{0} → {1}")
        @MethodSource("getErrors")
        @DisplayName("[T6-19 보강] 미등록 이벤트 404, Redis 장애 503")
        void errors(ErrorCode errorCode, int httpStatus, String code) throws Exception {
            // Given
            willThrow(new ApplicationException(errorCode)).given(queryService).event(anyLong());

            // When
            ResultActions result = mockMvc.perform(get(EVENT_PATH, "5"));

            // Then
            result.andExpect(status().is(httpStatus))
                    .andExpect(jsonPath("$.code").value(code));
        }

        @ParameterizedTest(name = "eventId={0}")
        @ValueSource(strings = {"abc", "1.5", "-1"})
        @DisplayName("[T6-19 보강] 숫자가 아닌 eventId 는 404 COMMON_NOT_FOUND, 서비스는 호출되지 않는다")
        void nonNumericEventId(String eventId) throws Exception {
            // When
            ResultActions result = mockMvc.perform(get(EVENT_PATH, eventId));

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(queryService);
        }
    }

    @Nested
    @DisplayName("POST /api/coupons/events/{eventId}/initialize — 재초기화")
    class Reinitialize {

        @Test
        @DisplayName("[T6-19] ADMIN 이면 204 본문 없음, Command 에 Requester·eventId 가 담긴다")
        void noContent() throws Exception {
            // When
            ResultActions result = reinitialize("5", "7", "ADMIN");

            // Then
            result.andExpect(status().isNoContent())
                    .andExpect(content().string(""));
            ArgumentCaptor<CouponReinitializeCommand> captor = ArgumentCaptor.forClass(CouponReinitializeCommand.class);
            verify(eventService).reinitialize(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new CouponReinitializeCommand(new Requester(7L, "ADMIN"), 5L));
        }

        @Test
        @DisplayName("[T6-19] USER 역할이면 AdminGuard 의 FORBIDDEN 이 403 COMMON_FORBIDDEN 으로 응답된다")
        void forbidden() throws Exception {
            // Given
            willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN)).given(eventService).reinitialize(any());

            // When
            ResultActions result = reinitialize("5", "7", "USER");

            // Then
            result.andExpect(status().isForbidden())
                    .andExpect(jsonPath("$.code").value("COMMON_FORBIDDEN"));
            ArgumentCaptor<CouponReinitializeCommand> captor = ArgumentCaptor.forClass(CouponReinitializeCommand.class);
            verify(eventService).reinitialize(captor.capture());
            assertThat(captor.getValue().requester()).isEqualTo(new Requester(7L, "USER"));
        }

        static Stream<Arguments> reinitErrors() {
            return Stream.of(
                    Arguments.of(CouponErrorCode.EVENT_NOT_FOUND, 404, "COUPON_EVENT_NOT_FOUND"),
                    Arguments.of(CouponErrorCode.TEMPORARILY_FAILED, 503, "COUPON_TEMPORARILY_FAILED"));
        }

        @ParameterizedTest(name = "{0} → {1}")
        @MethodSource("reinitErrors")
        @DisplayName("[T6-19 보강] 미등록 이벤트 404, Redis 장애 503")
        void errors(ErrorCode errorCode, int httpStatus, String code) throws Exception {
            // Given
            willThrow(new ApplicationException(errorCode)).given(eventService).reinitialize(any());

            // When
            ResultActions result = reinitialize("5", "7", "ADMIN");

            // Then
            result.andExpect(status().is(httpStatus))
                    .andExpect(jsonPath("$.code").value(code));
        }

        @Test
        @DisplayName("[T6-19 보강] 헤더가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingHeaders() throws Exception {
            // When
            ResultActions result = reinitialize("5", null, null);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(eventService);
        }

        @ParameterizedTest(name = "eventId={0}")
        @ValueSource(strings = {"abc", "-1"})
        @DisplayName("[T6-19 보강] 숫자가 아닌 eventId 는 ADMIN 헤더가 있어도 404 COMMON_NOT_FOUND")
        void nonNumericEventId(String eventId) throws Exception {
            // When
            ResultActions result = reinitialize(eventId, "7", "ADMIN");

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(eventService);
        }
    }
}
