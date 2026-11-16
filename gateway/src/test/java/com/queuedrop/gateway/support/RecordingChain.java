package com.queuedrop.gateway.support;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/** 다음 체인이 받은 요청을 기록하는 GatewayFilterChain (다운스트림 서버 대신) */
public final class RecordingChain implements GatewayFilterChain {

    private ServerHttpRequest received;
    private int calls;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange) {
        return Mono.fromRunnable(() -> {
            calls++;
            received = exchange.getRequest();
        });
    }

    public boolean called() {
        return calls > 0;
    }

    public int calls() {
        return calls;
    }

    public ServerHttpRequest received() {
        return received;
    }
}
