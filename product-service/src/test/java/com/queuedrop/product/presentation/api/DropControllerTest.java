package com.queuedrop.product.presentation.api;

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
import com.queuedrop.product.application.dto.DropPageResult;
import com.queuedrop.product.application.dto.DropRegisterCommand;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.DropSearchQuery;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.service.DropQueryService;
import com.queuedrop.product.application.service.DropService;
import com.queuedrop.product.application.service.ProductService;
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
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/** presentation 슬라이스. domain 타입(DropStatus 등)은 import 하지 않는다 — status 는 문자열로만 다룬다. */
@WebMvcTest({DropController.class, ProductController.class})
@Import({GlobalExceptionHandler.class, RequesterResolver.class})
class DropControllerTest {

    private static final Instant OPEN_AT = Instant.parse("2026-12-01T01:00:00Z");     // = 10:00+09:00
    private static final Instant CLOSE_AT = Instant.parse("2026-12-01T03:00:00Z");    // = 12:00+09:00
    private static final Instant CREATED_AT = Instant.parse("2026-11-16T10:00:00Z");

    private static final String VALID_BODY = """
            {"title":"한정판 스니커즈 드로우","entryCapacity":100,"winnerCount":10,
             "openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}""";

    @Autowired MockMvc mockMvc;
    @MockitoBean DropService dropService;
    @MockitoBean DropQueryService dropQueryService;
    @MockitoBean ProductService productService;

    private static DropResult dropResult(Long id, String status) {
        return new DropResult(id, 3L, "한정판 스니커즈 드로우", 100, 10, OPEN_AT, CLOSE_AT, status, CREATED_AT);
    }

    private ResultActions register(String productId, String userId, String role, String body) throws Exception {
        var request = post("/api/products/{productId}/drops", productId)
                .contentType(MediaType.APPLICATION_JSON).content(body);
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        if (role != null) {
            request.header("X-User-Role", role);
        }
        return mockMvc.perform(request);
    }

    @Nested
    @DisplayName("POST /api/products/{productId}/drops")
    class Register {

        @Test
        @DisplayName("[T2-24] ADMIN 이 등록하면 201 과 DropResponse(status 문자열), +09:00 오프셋 시각이 같은 Instant 로 Command 에 담긴다")
        void adminRegisters() throws Exception {
            // Given
            given(dropService.register(any())).willReturn(dropResult(11L, "SCHEDULED"));

            // When
            ResultActions result = register("3", "7", "ADMIN", VALID_BODY);

            // Then: 응답
            result.andExpect(status().isCreated())
                    .andExpect(jsonPath("$.id").value(11))
                    .andExpect(jsonPath("$.productId").value(3))
                    .andExpect(jsonPath("$.title").value("한정판 스니커즈 드로우"))
                    .andExpect(jsonPath("$.entryCapacity").value(100))
                    .andExpect(jsonPath("$.winnerCount").value(10))
                    .andExpect(jsonPath("$.openAt").value("2026-12-01T01:00:00Z"))
                    .andExpect(jsonPath("$.closeAt").value("2026-12-01T03:00:00Z"))
                    .andExpect(jsonPath("$.status").value("SCHEDULED"))
                    .andExpect(jsonPath("$.createdAt").value("2026-11-16T10:00:00Z"));

            // Then: Command (헤더 → Requester, 경로 → productId, 오프셋 → UTC Instant)
            ArgumentCaptor<DropRegisterCommand> captor = ArgumentCaptor.forClass(DropRegisterCommand.class);
            verify(dropService).register(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new DropRegisterCommand(
                    new Requester(7L, "ADMIN"), 3L, "한정판 스니커즈 드로우", 100, 10, OPEN_AT, CLOSE_AT));
        }

        @Test
        @DisplayName("[T2-24] USER 이면 서비스의 FORBIDDEN 이 403 COMMON_FORBIDDEN 과 고정 메시지로 매핑되고 Requester 는 헤더 그대로다")
        void userForbidden() throws Exception {
            // Given
            given(dropService.register(any())).willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN));

            // When
            ResultActions result = register("3", "8", "USER", VALID_BODY);

            // Then
            result.andExpect(status().isForbidden())
                    .andExpect(jsonPath("$.code").value("COMMON_FORBIDDEN"))
                    .andExpect(jsonPath("$.message").value(CommonErrorCode.FORBIDDEN.message()));
            ArgumentCaptor<DropRegisterCommand> captor = ArgumentCaptor.forClass(DropRegisterCommand.class);
            verify(dropService).register(captor.capture());
            assertThat(captor.getValue().requester()).isEqualTo(new Requester(8L, "USER"));
        }

        @Test
        @DisplayName("[T2-24] 도메인 검증 실패(INVALID_DROP)는 400 PRODUCT_INVALID_DROP, message 는 고정 문구이고 원인 상세는 없다")
        void invalidDropMapped() throws Exception {
            // Given
            given(dropService.register(any())).willThrow(new ApplicationException(
                    ProductErrorCode.INVALID_DROP, new IllegalArgumentException("응모 정원은 당첨 수량 이상이어야 합니다")));

            // When
            ResultActions result = register("3", "7", "ADMIN", VALID_BODY);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("PRODUCT_INVALID_DROP"))
                    .andExpect(jsonPath("$.message").value(ProductErrorCode.INVALID_DROP.message()));
        }

        @Test
        @DisplayName("[T2-24] 없는 상품(PRODUCT_NOT_FOUND)은 404 와 고정 메시지")
        void productNotFoundMapped() throws Exception {
            // Given
            given(dropService.register(any())).willThrow(new ApplicationException(ProductErrorCode.PRODUCT_NOT_FOUND));

            // When
            ResultActions result = register("999", "7", "ADMIN", VALID_BODY);

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("PRODUCT_NOT_FOUND"))
                    .andExpect(jsonPath("$.message").value(ProductErrorCode.PRODUCT_NOT_FOUND.message()));
        }

        @ParameterizedTest(name = "[T2-24] 헤더 userId={0}, role={1}")
        @org.junit.jupiter.params.provider.CsvSource(nullValues = "null", value = {"null, null", "7, null", "null, ADMIN"})
        @DisplayName("[T2-24] X-User-* 헤더가 하나라도 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingHeaders(String userId, String role) throws Exception {
            // When
            ResultActions result = register("3", userId, role, VALID_BODY);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(dropService);
        }

        static Stream<Arguments> invalidBodies() {
            return Stream.of(
                    Arguments.of("entryCapacity 누락", "entryCapacity", """
                            {"title":"t","winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("entryCapacity=0", "entryCapacity", """
                            {"title":"t","entryCapacity":0,"winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("winnerCount 누락", "winnerCount", """
                            {"title":"t","entryCapacity":100,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("winnerCount=-1", "winnerCount", """
                            {"title":"t","entryCapacity":100,"winnerCount":-1,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("title 누락", "title", """
                            {"entryCapacity":100,"winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("title 공백", "title", """
                            {"title":"   ","entryCapacity":100,"winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("title 101자", "title", """
                            {"title":"%s","entryCapacity":100,"winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00","closeAt":"2026-12-01T12:00:00+09:00"}"""
                            .formatted("가".repeat(101))),
                    Arguments.of("openAt 누락", "openAt", """
                            {"title":"t","entryCapacity":100,"winnerCount":10,"closeAt":"2026-12-01T12:00:00+09:00"}"""),
                    Arguments.of("closeAt 누락", "closeAt", """
                            {"title":"t","entryCapacity":100,"winnerCount":10,"openAt":"2026-12-01T10:00:00+09:00"}"""));
        }

        @ParameterizedTest(name = "[T2-25] {0}")
        @MethodSource("invalidBodies")
        @DisplayName("[T2-25] 필드 누락·0 이하·title 공백/길이 초과면 400 COMMON_INVALID_INPUT 과 해당 fieldError, 서비스는 호출되지 않는다")
        void invalidBody(String label, String field, String body) throws Exception {
            // When
            ResultActions result = register("3", "7", "ADMIN", body);

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                    .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem(field)));
            verifyNoInteractions(dropService);
        }

        @Test
        @DisplayName("[T2-25] openAt 이 시각 형식이 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void unparsableOpenAt() throws Exception {
            // When
            ResultActions result = register("3", "7", "ADMIN", """
                    {"title":"t","entryCapacity":100,"winnerCount":10,"openAt":"내일 10시","closeAt":"2026-12-01T12:00:00+09:00"}""");

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(dropService);
        }
    }

    @Nested
    @DisplayName("GET /api/products/drops")
    class Search {

        @Test
        @DisplayName("[T2-26] status=OPEN 이면 200, 페이지 구조와 status 문자열을 돌려주고 쿼리가 그대로 전달된다 (헤더 없이 공개)")
        void searchOpen() throws Exception {
            // Given
            given(dropQueryService.search(any())).willReturn(new DropPageResult(
                    List.of(dropResult(11L, "OPEN"), dropResult(12L, "OPEN")), 42L, 1, 2));

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops")
                    .param("status", "OPEN").param("page", "1").param("size", "2"));

            // Then: 응답
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.content.length()").value(2))
                    .andExpect(jsonPath("$.content[0].id").value(11))
                    .andExpect(jsonPath("$.content[0].status").value("OPEN"))
                    .andExpect(jsonPath("$.content[0].openAt").value("2026-12-01T01:00:00Z"))
                    .andExpect(jsonPath("$.content[1].id").value(12))
                    .andExpect(jsonPath("$.totalElements").value(42))
                    .andExpect(jsonPath("$.page").value(1))
                    .andExpect(jsonPath("$.size").value(2));

            // Then: Query
            ArgumentCaptor<DropSearchQuery> captor = ArgumentCaptor.forClass(DropSearchQuery.class);
            verify(dropQueryService).search(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new DropSearchQuery("OPEN", 1, 2));
        }

        @Test
        @DisplayName("[T2-26] 파라미터가 없으면 status=null, page=0, size=20 으로 조회한다")
        void searchDefaults() throws Exception {
            // Given
            given(dropQueryService.search(any())).willReturn(new DropPageResult(List.of(), 0L, 0, 20));

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.content").isEmpty())
                    .andExpect(jsonPath("$.totalElements").value(0));
            ArgumentCaptor<DropSearchQuery> captor = ArgumentCaptor.forClass(DropSearchQuery.class);
            verify(dropQueryService).search(captor.capture());
            assertThat(captor.getValue()).isEqualTo(new DropSearchQuery(null, 0, 20));
        }

        @Test
        @DisplayName("[T2-26] size=100 은 허용된다 (상한 경계)")
        void sizeUpperBoundAccepted() throws Exception {
            // Given
            given(dropQueryService.search(any())).willReturn(new DropPageResult(List.of(), 0L, 0, 100));

            // When / Then
            mockMvc.perform(get("/api/products/drops").param("size", "100"))
                    .andExpect(status().isOk());
        }

        @ParameterizedTest(name = "[T2-26] {0}")
        @ValueSource(strings = {"page=-1", "size=0", "size=101", "page=abc"})
        @DisplayName("[T2-26] page/size 범위·형식 위반이면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidPaging(String param) throws Exception {
            // Given
            String[] kv = param.split("=");

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops").param(kv[0], kv[1]));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(dropQueryService);
        }

        @Test
        @DisplayName("[T2-26] 서비스의 INVALID_STATUS_FILTER 는 400 PRODUCT_INVALID_STATUS_FILTER 와 고정 메시지")
        void invalidStatusFilterMapped() throws Exception {
            // Given
            given(dropQueryService.search(any()))
                    .willThrow(new ApplicationException(ProductErrorCode.INVALID_STATUS_FILTER));

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops").param("status", "FOO"));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("PRODUCT_INVALID_STATUS_FILTER"))
                    .andExpect(jsonPath("$.message").value(ProductErrorCode.INVALID_STATUS_FILTER.message()));
        }
    }

    @Nested
    @DisplayName("GET /api/products/drops/{dropId}")
    class Get {

        @Test
        @DisplayName("[T2-26] 있는 dropId 면 200 과 DropResponse (헤더 없이 공개)")
        void getDrop() throws Exception {
            // Given
            given(dropQueryService.get(11L)).willReturn(dropResult(11L, "CLOSED"));

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops/{dropId}", 11));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(11))
                    .andExpect(jsonPath("$.productId").value(3))
                    .andExpect(jsonPath("$.status").value("CLOSED"))
                    .andExpect(jsonPath("$.closeAt").value("2026-12-01T03:00:00Z"));
        }

        @Test
        @DisplayName("[T2-26] 없는 dropId(DROP_NOT_FOUND)는 404 PRODUCT_DROP_NOT_FOUND 와 고정 메시지")
        void dropNotFoundMapped() throws Exception {
            // Given
            given(dropQueryService.get(anyLong())).willThrow(new ApplicationException(ProductErrorCode.DROP_NOT_FOUND));

            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops/{dropId}", 999));

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("PRODUCT_DROP_NOT_FOUND"))
                    .andExpect(jsonPath("$.message").value(ProductErrorCode.DROP_NOT_FOUND.message()));
        }

        @Test
        @DisplayName("[T2-26] 숫자가 아닌 경로 변수 /drops/abc 는 404, 서비스는 호출되지 않는다")
        void nonNumericDropId() throws Exception {
            // When
            ResultActions result = mockMvc.perform(get("/api/products/drops/abc"));

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("COMMON_NOT_FOUND"));
            verifyNoInteractions(dropQueryService);
        }
    }

    @Nested
    @DisplayName("POST /api/products/drops/{dropId}/republish")
    class Republish {

        @Test
        @DisplayName("[T2-24] ADMIN 이 재발행하면 202 이고 본문이 없으며, 헤더의 Requester 와 dropId 가 그대로 전달된다")
        void adminRepublishes() throws Exception {
            // When
            ResultActions result = mockMvc.perform(post("/api/products/drops/{dropId}/republish", 11)
                    .header("X-User-Id", "7").header("X-User-Role", "ADMIN"));

            // Then
            result.andExpect(status().isAccepted())
                    .andExpect(content().string(""));
            ArgumentCaptor<Requester> requester = ArgumentCaptor.forClass(Requester.class);
            ArgumentCaptor<Long> dropId = ArgumentCaptor.forClass(Long.class);
            verify(dropService).republish(requester.capture(), dropId.capture());
            assertThat(requester.getValue()).isEqualTo(new Requester(7L, "ADMIN"));
            assertThat(dropId.getValue()).isEqualTo(11L);
        }

        @Test
        @DisplayName("[T2-24] USER 이면 403 COMMON_FORBIDDEN")
        void userForbidden() throws Exception {
            // Given
            willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN)).given(dropService).republish(any(), any());

            // When
            ResultActions result = mockMvc.perform(post("/api/products/drops/{dropId}/republish", 11)
                    .header("X-User-Id", "8").header("X-User-Role", "USER"));

            // Then
            result.andExpect(status().isForbidden())
                    .andExpect(jsonPath("$.code").value("COMMON_FORBIDDEN"))
                    .andExpect(jsonPath("$.message").value(CommonErrorCode.FORBIDDEN.message()));
        }

        @Test
        @DisplayName("[T2-24] 헤더가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingHeaders() throws Exception {
            // When
            ResultActions result = mockMvc.perform(post("/api/products/drops/{dropId}/republish", 11));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(dropService);
        }

        @Test
        @DisplayName("[T2-24] 없는 dropId 는 404 PRODUCT_DROP_NOT_FOUND")
        void dropNotFound() throws Exception {
            // Given
            willThrow(new ApplicationException(ProductErrorCode.DROP_NOT_FOUND)).given(dropService).republish(any(), any());

            // When
            ResultActions result = mockMvc.perform(post("/api/products/drops/{dropId}/republish", 999)
                    .header("X-User-Id", "7").header("X-User-Role", "ADMIN"));

            // Then
            result.andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("PRODUCT_DROP_NOT_FOUND"));
        }
    }
}
