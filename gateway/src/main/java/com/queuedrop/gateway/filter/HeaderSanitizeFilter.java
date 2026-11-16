package com.queuedrop.gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/** 클라이언트가 보낸 사용자 헤더를 항상 제거한다. JWT 검증보다 먼저 실행되어 어떤 경로에서도 위조 헤더가 남지 않게 한다. */
@Component
public class HeaderSanitizeFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest sanitized = exchange.getRequest().mutate()
                .headers(h -> {                       // HttpHeaders 는 대소문자 무시
                    h.remove(UserHeaders.USER_ID);
                    h.remove(UserHeaders.USER_ROLE);
                })
                .build();
        return chain.filter(exchange.mutate().request(sanitized).build());
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
