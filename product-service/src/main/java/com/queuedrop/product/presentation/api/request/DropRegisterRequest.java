package com.queuedrop.product.presentation.api.request;

import com.queuedrop.product.application.dto.DropRegisterCommand;
import com.queuedrop.product.application.dto.Requester;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * 형식 검증(필수·양수·길이)만 한다. 업무 규칙(M ≥ K, 시각 순서, 1분 여유)은 domain 이 담당한다.
 * 수량은 Integer 로 받아 누락을 0 이 아닌 "필수 누락"으로 구분한다.
 */
public record DropRegisterRequest(
        @NotBlank @Size(max = 100) String title,
        @NotNull @Positive Integer entryCapacity,
        @NotNull @Positive Integer winnerCount,
        @NotNull Instant openAt,      // ISO-8601, 예: 2026-12-01T10:00:00+09:00
        @NotNull Instant closeAt) {

    public DropRegisterCommand toCommand(Requester requester, Long productId) {
        return new DropRegisterCommand(requester, productId, title, entryCapacity, winnerCount, openAt, closeAt);
    }
}
