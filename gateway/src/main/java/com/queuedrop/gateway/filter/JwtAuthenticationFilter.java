package com.queuedrop.gateway.filter;

import com.queuedrop.gateway.auth.ErrorResponseWriter;
import com.queuedrop.gateway.auth.GatewayErrorCode;
import com.queuedrop.gateway.auth.JwtVerificationException;
import com.queuedrop.gateway.auth.JwtVerifier;
import com.queuedrop.gateway.auth.PublicPathMatcher;
import com.queuedrop.gateway.auth.VerifiedUser;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 토큰 검증 → X-User-Id / X-User-Role 주입. 보호 경로는 실패 시 401.
 * 공개 경로는 토큰이 없거나 잘못돼도 익명으로 통과하고, 유효하면 헤더를 주입한다.
 */
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

    private static final String BEARER = "Bearer ";

    private final JwtVerifier verifier;
    private final PublicPathMatcher publicPaths;
    private final ErrorResponseWriter errorWriter;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        boolean isPublic = publicPaths.isPublic(request.getMethod(), request.getPath().pathWithinApplication());
        String token = resolveToken(request);

        if (token == null) {
            return isPublic ? chain.filter(exchange)
                            : errorWriter.write(exchange, GatewayErrorCode.TOKEN_MISSING);
        }

        VerifiedUser user;
        try {
            user = verifier.verify(token);
        } catch (JwtVerificationException e) {
            return isPublic ? chain.filter(exchange)
                            : errorWriter.write(exchange, e.errorCode());
        }

        ServerHttpRequest authenticated = request.mutate()
                .headers(h -> {
                    h.set(UserHeaders.USER_ID, String.valueOf(user.userId()));
                    h.set(UserHeaders.USER_ROLE, user.role());
                })
                .build();
        return chain.filter(exchange.mutate().request(authenticated).build());
    }

    private String resolveToken(ServerHttpRequest request) {
        String header = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.regionMatches(true, 0, BEARER, 0, BEARER.length())) {
            return null;
        }
        String token = header.substring(BEARER.length()).trim();
        return token.isEmpty() ? null : token;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
