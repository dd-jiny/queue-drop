package com.queuedrop.gateway.filter;

import com.queuedrop.gateway.auth.ErrorResponseWriter;
import com.queuedrop.gateway.auth.JwtVerifier;
import com.queuedrop.gateway.auth.PublicPathMatcher;
import com.queuedrop.gateway.support.RecordingChain;
import com.queuedrop.gateway.support.TestTokens;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class JwtAuthenticationFilterTest {

    private static final String PROTECTED = "/api/entries/drops/1";
    private static final String PUBLIC = "/api/products/drops";

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final JwtAuthenticationFilter filter = new JwtAuthenticationFilter(
            new JwtVerifier(TestTokens.authProperties()),
            new PublicPathMatcher(TestTokens.authProperties()),
            new ErrorResponseWriter(jsonMapper));

    // ---- 보호 경로 ------------------------------------------------------------

    @Test
    @DisplayName("[T1-20] 보호 경로에 토큰이 없으면 401 AUTH_TOKEN_MISSING, 체인은 호출되지 않는다")
    void protectedWithoutToken() {
        // Given
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.post(PROTECTED));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isFalse();
        assertUnauthorized(exchange, "AUTH_TOKEN_MISSING", "인증 토큰이 필요합니다.");
    }

    @Test
    @DisplayName("[T1-20] 경계: 'Bearer ' 뒤가 비어 있으면 토큰 없음으로 보고 401 AUTH_TOKEN_MISSING")
    void protectedWithEmptyBearer() {
        // Given
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.post(PROTECTED)
                .header(HttpHeaders.AUTHORIZATION, "Bearer "));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isFalse();
        assertUnauthorized(exchange, "AUTH_TOKEN_MISSING", "인증 토큰이 필요합니다.");
    }

    @Test
    @DisplayName("[T1-20] 경계: Bearer 가 아닌 스킴(Basic)은 토큰 없음으로 보고 401 AUTH_TOKEN_MISSING")
    void protectedWithOtherScheme() {
        // Given
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.post(PROTECTED)
                .header(HttpHeaders.AUTHORIZATION, "Basic dXNlcjpwYXNz"));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isFalse();
        assertUnauthorized(exchange, "AUTH_TOKEN_MISSING", "인증 토큰이 필요합니다.");
    }

    @Test
    @DisplayName("[T1-21] 보호 경로에 만료 토큰이면 401 AUTH_EXPIRED_TOKEN")
    void protectedWithExpiredToken() {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.post(PROTECTED),
                TestTokens.expired(42L, "USER")));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isFalse();
        assertUnauthorized(exchange, "AUTH_EXPIRED_TOKEN", "인증 토큰이 만료되었습니다.");
    }

    static Stream<Arguments> invalidTokens() {
        return Stream.of(
                Arguments.of("다른 키로 서명", TestTokens.signedWithOtherKey(42L, "USER")),
                Arguments.of("형식 깨짐", TestTokens.malformed()),
                Arguments.of("다른 issuer", TestTokens.otherIssuer(42L, "USER")),
                Arguments.of("role 클레임 없음", TestTokens.withoutRole(42L)),
                Arguments.of("alg=none 서명 없음", TestTokens.unsigned(42L, "ADMIN")),
                Arguments.of("sub 가 숫자 아님", TestTokens.withSubject("abc", "USER")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTokens")
    @DisplayName("[T1-22] 보호 경로에 위조·형식 오류·다른 issuer·role 없는·서명 없는(alg=none)·sub 비숫자 토큰이면 401 AUTH_INVALID_TOKEN")
    void protectedWithInvalidToken(String caseName, String token) {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.post(PROTECTED), token));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isFalse();
        assertUnauthorized(exchange, "AUTH_INVALID_TOKEN", "유효하지 않은 인증 토큰입니다.");
    }

    @Test
    @DisplayName("[T1-23] 보호 경로에 유효 토큰(sub=42, role=USER)이면 체인 요청에 X-User-Id: 42, X-User-Role: USER")
    void protectedWithValidToken() {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.post(PROTECTED),
                TestTokens.valid(42L, "USER")));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.calls()).isEqualTo(1);
        HttpHeaders headers = chain.received().getHeaders();
        assertThat(headers.get(UserHeaders.USER_ID)).containsExactly("42");
        assertThat(headers.get(UserHeaders.USER_ROLE)).containsExactly("USER");
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("[T1-23] 경계: 소문자 'bearer ' 접두어도 인식한다")
    void lowercaseBearerPrefix() {
        // Given
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.post(PROTECTED)
                .header(HttpHeaders.AUTHORIZATION, "bearer " + TestTokens.valid(42L, "ADMIN")));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isTrue();
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ID)).containsExactly("42");
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ROLE)).containsExactly("ADMIN");
    }

    @Test
    @DisplayName("[T1-23] 경계: 제거 필터 없이도 위조 X-User-Id/X-User-Role 은 토큰 값으로 덮어써져 값이 하나만 남는다")
    void overwritesForgedHeadersEvenWithoutSanitize() {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.post(PROTECTED), TestTokens.valid(42L, "USER"))
                .header(UserHeaders.USER_ID, "1")
                .header(UserHeaders.USER_ROLE, "ADMIN"));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ID)).containsExactly("42");
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ROLE)).containsExactly("USER");
    }

    // ---- 공개 경로 ------------------------------------------------------------

    @Test
    @DisplayName("[T1-24] 공개 경로에 토큰이 없으면 체인 호출, 사용자 헤더 없음")
    void publicWithoutToken() {
        // Given
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.get(PUBLIC));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertAnonymousPass(exchange, chain);
    }

    static Stream<Arguments> badTokensOnPublic() {
        return Stream.of(
                Arguments.of("형식 깨짐", TestTokens.malformed()),
                Arguments.of("다른 키로 서명", TestTokens.signedWithOtherKey(42L, "USER")),
                Arguments.of("만료", TestTokens.expired(42L, "USER")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badTokensOnPublic")
    @DisplayName("[T1-24] 공개 경로에 잘못된/만료된 토큰이면 401 이 아니라 익명으로 통과한다")
    void publicWithBadToken(String caseName, String token) {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.get(PUBLIC), token));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertAnonymousPass(exchange, chain);
    }

    @Test
    @DisplayName("[T1-24] 경계: 공개 경로에 유효 토큰이면 헤더를 주입하고 통과한다")
    void publicWithValidToken() {
        // Given
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.get(PUBLIC),
                TestTokens.valid(7L, "USER")));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.calls()).isEqualTo(1);
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ID)).containsExactly("7");
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ROLE)).containsExactly("USER");
    }

    // ---- 두 필터 연결 --------------------------------------------------------

    @Test
    @DisplayName("[T1-25] 위조 X-User-Id: 1 + 유효 토큰(sub=42) → 제거 필터 → 검증 필터 순서로 거치면 최종 X-User-Id 는 42 하나")
    void sanitizeThenAuthenticate() {
        // Given
        HeaderSanitizeFilter sanitize = new HeaderSanitizeFilter();
        assertThat(sanitize.getOrder()).isLessThan(filter.getOrder());   // 실제 GlobalFilter 정렬 순서와 같게 연결
        MockServerWebExchange exchange = exchange(bearer(MockServerHttpRequest.post(PROTECTED), TestTokens.valid(42L, "USER"))
                .header(UserHeaders.USER_ID, "1")
                .header("x-user-role", "ADMIN"));
        RecordingChain downstream = new RecordingChain();
        GatewayFilterChain chain = ex -> filter.filter(ex, downstream);

        // When
        sanitize.filter(exchange, chain).block();

        // Then
        assertThat(downstream.calls()).isEqualTo(1);
        assertThat(downstream.received().getHeaders().get(UserHeaders.USER_ID)).containsExactly("42");
        assertThat(downstream.received().getHeaders().get(UserHeaders.USER_ROLE)).containsExactly("USER");
    }

    @Test
    @DisplayName("[T1-25] 경계: 공개 경로 + 토큰 없음 + 위조 헤더 → 두 필터를 거치면 사용자 헤더 없음")
    void sanitizeThenPublicAnonymous() {
        // Given
        HeaderSanitizeFilter sanitize = new HeaderSanitizeFilter();
        MockServerWebExchange exchange = exchange(MockServerHttpRequest.get(PUBLIC)
                .header(UserHeaders.USER_ID, "1")
                .header(UserHeaders.USER_ROLE, "ADMIN"));
        RecordingChain downstream = new RecordingChain();
        GatewayFilterChain chain = ex -> filter.filter(ex, downstream);

        // When
        sanitize.filter(exchange, chain).block();

        // Then
        assertAnonymousPass(exchange, downstream);
    }

    // ---- helpers ------------------------------------------------------------

    private static MockServerWebExchange exchange(MockServerHttpRequest.BaseBuilder<?> request) {
        return MockServerWebExchange.from(request);
    }

    private static MockServerHttpRequest.BaseBuilder<?> bearer(MockServerHttpRequest.BaseBuilder<?> request, String token) {
        return request.header(HttpHeaders.AUTHORIZATION, "Bearer " + token);
    }

    private static void assertAnonymousPass(MockServerWebExchange exchange, RecordingChain chain) {
        assertThat(chain.calls()).isEqualTo(1);
        assertThat(chain.received().getHeaders().containsHeader(UserHeaders.USER_ID)).isFalse();
        assertThat(chain.received().getHeaders().containsHeader(UserHeaders.USER_ROLE)).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    private void assertUnauthorized(MockServerWebExchange exchange, String code, String message) {
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);

        String body = exchange.getResponse().getBodyAsString().block();
        JsonNode json = jsonMapper.readTree(body);
        assertThat(json.get("code").asString()).isEqualTo(code);
        assertThat(json.get("message").asString()).isEqualTo(message);
        assertThat(json.get("fieldErrors").isArray()).isTrue();
        assertThat(json.get("fieldErrors").isEmpty()).isTrue();
        assertThat(Instant.parse(json.get("timestamp").asString()))
                .isCloseTo(Instant.now(), within(java.time.Duration.ofMinutes(1)));
    }
}
