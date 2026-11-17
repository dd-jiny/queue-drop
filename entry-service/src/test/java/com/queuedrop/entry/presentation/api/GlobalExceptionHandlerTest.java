package com.queuedrop.entry.presentation.api;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * T0-05: 전역 예외 처리기의 에러 코드 → HTTP 매핑 (phase-0 3.8 "Spring MVC 표준 예외 매핑" 표, layered-architecture 4.5).
 * 기대 코드·메시지는 단계 문서 3.3 의 CommonErrorCode 값을 문자열로 고정해 문서와의 불일치를 잡는다.
 *
 * 테스트 전용 컨트롤러는 이 클래스의 static 중첩 클래스로 두고 @Import 로만 올린다.
 * 테스트 클래스의 중첩 클래스는 Spring Boot TestTypeExcludeFilter 가 컴포넌트 스캔에서 제외하므로
 * 이후 @SpringBootTest(ApplicationContextTest) 컨텍스트에는 등록되지 않는다.
 */
@WebMvcTest(controllers = GlobalExceptionHandlerTest.ExceptionTestController.class)
@Import({GlobalExceptionHandlerTest.ExceptionTestController.class, SimpleMeterRegistry.class})
@DisplayName("[T0-05] GlobalExceptionHandler 에러 응답 매핑")
class GlobalExceptionHandlerTest {

    private static final String BASE = "/test/errors";
    private static final String INTERNAL_SECRET = "db-password=s3cr3t at internal.host:5432";
    private static final String CAUSE_SECRET = "Entry(userId=42) quota exceeded: internal-detail-xyz";

    private static final String INVALID_INPUT_MSG = "요청 값이 올바르지 않습니다.";

    @Autowired
    MockMvc mockMvc;

    // ---------------------------------------------------------------- ApplicationException

    @Test
    @DisplayName("[T0-05] ApplicationException(NOT_FOUND) 은 404 COMMON_NOT_FOUND 로 매핑된다")
    void applicationException() throws Exception {
        // Given: NOT_FOUND 를 던지는 엔드포인트
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/application"));

        // Then
        result.andExpect(status().isNotFound());
        expectErrorBody(result, "COMMON_NOT_FOUND", "대상을 찾을 수 없습니다.");
        result.andExpect(jsonPath("$.fieldErrors", hasSize(0)));
    }

    @Test
    @DisplayName("[T0-05] 서비스별 ErrorCode(503) 를 가진 ApplicationException 하위 예외는 503 + 해당 code·고정 message 로 매핑된다")
    void applicationExceptionSubclassWithServiceErrorCode() throws Exception {
        // Given: TestErrorCode.UNAVAILABLE(503) 을 가진 하위 예외를 던지는 엔드포인트
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/service-unavailable"));

        // Then
        result.andExpect(status().isServiceUnavailable());
        expectErrorBody(result, "TEST_UNAVAILABLE", "테스트 서비스를 일시적으로 사용할 수 없습니다.");
        result.andExpect(jsonPath("$.fieldErrors", hasSize(0)));
    }

    @Test
    @DisplayName("[T0-05] 4xx ApplicationException 의 cause 메시지는 응답 본문에 노출되지 않는다")
    void applicationExceptionCauseNotLeaked() throws Exception {
        // Given: cause 에 내부 상세를 담은 4xx ApplicationException
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/application-with-cause"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
        result.andExpect(content().string(not(containsString(CAUSE_SECRET))))
                .andExpect(content().string(not(containsString("IllegalArgumentException"))));
    }

    // ---------------------------------------------------------------- 검증

    @Test
    @DisplayName("[T0-05] @Valid 검증 실패는 400 COMMON_INVALID_INPUT 과 필드별 fieldErrors 로 매핑된다")
    void validationFailure() throws Exception {
        // Given: name 공백, quantity 음수
        String body = """
                {"name": " ", "quantity": -1}
                """;

        // When
        ResultActions result = mockMvc.perform(post(BASE + "/validation")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
        result.andExpect(jsonPath("$.fieldErrors", hasSize(2)))
                .andExpect(jsonPath("$.fieldErrors[*].field", containsInAnyOrder("name", "quantity")))
                .andExpect(jsonPath("$.fieldErrors[0].reason").isNotEmpty())
                .andExpect(jsonPath("$.fieldErrors[1].reason").isNotEmpty());
    }

    @Test
    @DisplayName("[T0-05] @RequestParam @Min(1) 위반은 400 COMMON_INVALID_INPUT 과 파라미터 이름을 field 로 가진 fieldErrors 1개로 매핑된다")
    void requestParamConstraintViolation() throws Exception {
        // Given: size 는 @Min(1)
        // When: size=0
        ResultActions result = mockMvc.perform(get(BASE + "/page").param("size", "0"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
        result.andExpect(jsonPath("$.fieldErrors", hasSize(1)))
                .andExpect(jsonPath("$.fieldErrors[0].field").value("size"))
                .andExpect(jsonPath("$.fieldErrors[0].reason").isNotEmpty());
    }

    // ---------------------------------------------------------------- 헤더

    @Test
    @DisplayName("[T0-05] X-User-Id 헤더 누락은 400 COMMON_MISSING_USER_HEADER 로 매핑된다")
    void missingUserHeader() throws Exception {
        // Given: X-User-Id 를 요구하는 엔드포인트
        // When: 헤더 없이 호출
        ResultActions result = mockMvc.perform(get(BASE + "/header"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_MISSING_USER_HEADER", "사용자 식별 헤더가 없습니다.");
        result.andExpect(jsonPath("$.fieldErrors", hasSize(0)));
    }

    @Test
    @DisplayName("[T0-05] X-User-Role 헤더 누락도 400 COMMON_MISSING_USER_HEADER 로 매핑된다")
    void missingUserRoleHeader() throws Exception {
        // Given: X-User-Role 을 요구하는 엔드포인트
        // When: 헤더 없이 호출
        ResultActions result = mockMvc.perform(get(BASE + "/role-header"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_MISSING_USER_HEADER", "사용자 식별 헤더가 없습니다.");
    }

    @Test
    @DisplayName("[T0-05] X-User-* 가 아닌 필수 헤더 누락은 400 COMMON_INVALID_INPUT 으로 매핑된다")
    void missingNonUserHeader() throws Exception {
        // Given: X-Request-Source 를 요구하는 엔드포인트
        // When: 헤더 없이 호출
        ResultActions result = mockMvc.perform(get(BASE + "/source-header"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
    }

    // ---------------------------------------------------------------- 요청 형식

    @Test
    @DisplayName("[T0-05] 깨진 JSON 본문은 500 이 아니라 400 COMMON_INVALID_INPUT 으로 매핑된다")
    void malformedJson() throws Exception {
        // Given
        String broken = "{\"name\": \"a\", ";

        // When
        ResultActions result = mockMvc.perform(post(BASE + "/validation")
                .contentType(MediaType.APPLICATION_JSON)
                .content(broken));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
        result.andExpect(jsonPath("$.fieldErrors", hasSize(0)));
    }

    @Test
    @DisplayName("[T0-05] 지원하지 않는 Content-Type(text/plain) 본문은 415 COMMON_UNSUPPORTED_MEDIA_TYPE 으로 매핑된다")
    void unsupportedMediaType() throws Exception {
        // Given: JSON 본문을 받는 엔드포인트
        // When: text/plain 으로 전송
        ResultActions result = mockMvc.perform(post(BASE + "/validation")
                .contentType(MediaType.TEXT_PLAIN)
                .content("name=a"));

        // Then
        result.andExpect(status().isUnsupportedMediaType());
        expectErrorBody(result, "COMMON_UNSUPPORTED_MEDIA_TYPE", "지원하지 않는 콘텐츠 형식입니다.");
    }

    @Test
    @DisplayName("[T0-05] 경로 변수 타입 불일치는 400 COMMON_INVALID_INPUT 으로 매핑된다")
    void pathVariableTypeMismatch() throws Exception {
        // Given: Long 경로 변수
        // When: 숫자가 아닌 값
        ResultActions result = mockMvc.perform(get(BASE + "/path/abc"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
    }

    @Test
    @DisplayName("[T0-05] 필수 쿼리 파라미터 누락은 400 COMMON_INVALID_INPUT 으로 매핑된다")
    void missingRequestParam() throws Exception {
        // Given: 필수 쿼리 page
        // When: page 없이 호출
        ResultActions result = mockMvc.perform(get(BASE + "/param"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
    }

    @Test
    @DisplayName("[T0-05] 없는 경로는 404 COMMON_NOT_FOUND 로 매핑된다")
    void noSuchPath() throws Exception {
        // Given: 매핑되지 않은 경로
        // When
        ResultActions result = mockMvc.perform(get("/no/such/path"));

        // Then
        result.andExpect(status().isNotFound());
        expectErrorBody(result, "COMMON_NOT_FOUND", "대상을 찾을 수 없습니다.");
    }

    @Test
    @DisplayName("[T0-05] 지원하지 않는 메서드는 405 COMMON_METHOD_NOT_ALLOWED 로 매핑된다")
    void methodNotAllowed() throws Exception {
        // Given: GET 만 허용하는 엔드포인트
        // When: DELETE 호출
        ResultActions result = mockMvc.perform(delete(BASE + "/get-only"));

        // Then
        result.andExpect(status().isMethodNotAllowed());
        expectErrorBody(result, "COMMON_METHOD_NOT_ALLOWED", "지원하지 않는 요청 메서드입니다.");
    }

    // ---------------------------------------------------------------- 표준 4xx 정규화 / 500

    @Test
    @DisplayName("[T0-05] ResponseStatusException(409) 같은 표준 4xx 는 400 COMMON_INVALID_INPUT 으로 정규화된다")
    void responseStatusConflictNormalizedTo400() throws Exception {
        // Given: ResponseStatusException(CONFLICT) 를 던지는 엔드포인트
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/status/conflict"));

        // Then
        result.andExpect(status().isBadRequest());
        expectErrorBody(result, "COMMON_INVALID_INPUT", INVALID_INPUT_MSG);
        result.andExpect(content().string(not(containsString(INTERNAL_SECRET))));
    }

    @Test
    @DisplayName("[T0-05] ResponseStatusException(403) 은 403 COMMON_FORBIDDEN 으로 매핑된다")
    void responseStatusForbidden() throws Exception {
        // Given: ResponseStatusException(FORBIDDEN) 를 던지는 엔드포인트
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/status/forbidden"));

        // Then
        result.andExpect(status().isForbidden());
        expectErrorBody(result, "COMMON_FORBIDDEN", "권한이 없습니다.");
    }

    @Test
    @DisplayName("[T0-05] 예상 못한 예외는 500 COMMON_INTERNAL_ERROR 고정 메시지로 매핑되고 예외 메시지는 응답에 노출되지 않는다")
    void unexpectedException() throws Exception {
        // Given: 내부 정보를 메시지로 가진 IllegalStateException 을 던지는 엔드포인트
        // When
        ResultActions result = mockMvc.perform(get(BASE + "/unexpected"));

        // Then
        result.andExpect(status().isInternalServerError());
        expectErrorBody(result, "COMMON_INTERNAL_ERROR", "일시적인 오류가 발생했습니다.");
        result.andExpect(content().string(not(containsString(INTERNAL_SECRET))))
                .andExpect(content().string(not(containsString("IllegalStateException"))));
    }

    /** 공통 에러 바디 형식: JSON, code·message 고정값, fieldErrors 배열, timestamp 존재 */
    private static void expectErrorBody(ResultActions result, String code, String message) throws Exception {
        result.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value(code))
                .andExpect(jsonPath("$.message").value(message))
                .andExpect(jsonPath("$.fieldErrors").isArray())
                .andExpect(jsonPath("$.timestamp").isNotEmpty());
    }

    // ================================================================ 테스트 전용 타입

    /** 서비스별 ErrorCode enum 을 흉내 낸 테스트 전용 코드 (예: EntryErrorCode) */
    enum TestErrorCode implements ErrorCode {
        UNAVAILABLE("TEST_UNAVAILABLE", 503, "테스트 서비스를 일시적으로 사용할 수 없습니다.");

        private final String code;
        private final int status;
        private final String message;

        TestErrorCode(String code, int status, String message) {
            this.code = code;
            this.status = status;
            this.message = message;
        }

        @Override public String code() { return code; }
        @Override public int status() { return status; }
        @Override public String message() { return message; }
    }

    /** 서비스별 예외(EntryApplyException 등)를 흉내 낸 ApplicationException 하위 예외 */
    static class TestUnavailableException extends ApplicationException {
        TestUnavailableException() {
            super(TestErrorCode.UNAVAILABLE);
        }
    }

    record SampleRequest(@NotBlank String name, @Positive int quantity) {}

    /** 예외를 의도적으로 발생시키는 테스트 전용 컨트롤러. 컴포넌트 스캔 대상이 아니며 @Import 로만 등록된다. */
    @RestController
    @RequestMapping(BASE)
    static class ExceptionTestController {

        @GetMapping("/application")
        void application() {
            throw new ApplicationException(CommonErrorCode.NOT_FOUND);
        }

        @GetMapping("/service-unavailable")
        void serviceUnavailable() {
            throw new TestUnavailableException();
        }

        @GetMapping("/application-with-cause")
        void applicationWithCause() {
            throw new ApplicationException(CommonErrorCode.INVALID_INPUT, new IllegalArgumentException(CAUSE_SECRET));
        }

        @PostMapping("/validation")
        void validation(@Valid @RequestBody SampleRequest request) {
        }

        @GetMapping("/page")
        void page(@RequestParam @Min(1) int size) {
        }

        @GetMapping("/header")
        void header(@RequestHeader("X-User-Id") Long userId) {
        }

        @GetMapping("/role-header")
        void roleHeader(@RequestHeader("X-User-Role") String role) {
        }

        @GetMapping("/source-header")
        void sourceHeader(@RequestHeader("X-Request-Source") String source) {
        }

        @GetMapping("/path/{id}")
        void pathType(@PathVariable Long id) {
        }

        @GetMapping("/param")
        void requiredParam(@RequestParam Long page) {
        }

        @GetMapping("/get-only")
        void getOnly() {
        }

        @GetMapping("/status/conflict")
        void conflict() {
            throw new ResponseStatusException(HttpStatus.CONFLICT, INTERNAL_SECRET);
        }

        @GetMapping("/status/forbidden")
        void forbidden() {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN);
        }

        @GetMapping("/unexpected")
        void unexpected() {
            throw new IllegalStateException(INTERNAL_SECRET);
        }
    }
}
