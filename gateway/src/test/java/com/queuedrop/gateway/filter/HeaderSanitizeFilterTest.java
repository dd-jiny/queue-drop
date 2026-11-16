package com.queuedrop.gateway.filter;

import com.queuedrop.gateway.support.RecordingChain;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderSanitizeFilterTest {

    private final HeaderSanitizeFilter filter = new HeaderSanitizeFilter();

    @Test
    @DisplayName("[T1-19] 클라이언트가 보낸 X-User-Id, x-user-role(소문자) 헤더는 다음 체인에 전달되지 않는다")
    void removesClientUserHeaders() {
        // Given
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/entries/drops/1")
                        .header("X-User-Id", "999")
                        .header("x-user-role", "ADMIN")
                        .header("X-Trace", "keep"));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.called()).isTrue();
        assertThat(chain.received().getHeaders().containsHeader(UserHeaders.USER_ID)).isFalse();
        assertThat(chain.received().getHeaders().containsHeader(UserHeaders.USER_ROLE)).isFalse();
        assertThat(chain.received().getHeaders().getFirst("X-Trace")).isEqualTo("keep");
    }

    @Test
    @DisplayName("[T1-19] 같은 헤더가 여러 값으로 와도 모두 제거된다")
    void removesAllValues() {
        // Given
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/entries/drops/1")
                        .header("X-User-Id", "1", "2")
                        .header("X-USER-ID", "3"));
        RecordingChain chain = new RecordingChain();

        // When
        filter.filter(exchange, chain).block();

        // Then
        assertThat(chain.received().getHeaders().get(UserHeaders.USER_ID)).isNullOrEmpty();
    }

    @Test
    @DisplayName("[T1-19] 제거 필터는 JWT 검증 필터보다 먼저 실행된다 (HIGHEST_PRECEDENCE)")
    void runsBeforeJwtFilter() {
        // Then
        assertThat(filter.getOrder()).isEqualTo(org.springframework.core.Ordered.HIGHEST_PRECEDENCE);
    }
}
