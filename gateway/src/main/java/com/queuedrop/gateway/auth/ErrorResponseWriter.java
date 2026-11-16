package com.queuedrop.gateway.auth;

import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.common.error.ErrorResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/** 인증 실패를 공통 ErrorResponse JSON 으로 응답한다 */
@Component
@RequiredArgsConstructor
public class ErrorResponseWriter {

    private final JsonMapper jsonMapper;     // Boot 4 자동 구성 (Jackson 3)

    public Mono<Void> write(ServerWebExchange exchange, ErrorCode code) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatusCode.valueOf(code.status()));
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = jsonMapper.writeValueAsBytes(ErrorResponse.of(code));
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}
