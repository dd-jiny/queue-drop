package com.queuedrop.product.presentation.api.response;

import com.queuedrop.product.application.dto.DropResult;

import java.time.Instant;

public record DropResponse(Long id, Long productId, String title, int entryCapacity, int winnerCount,
                           Instant openAt, Instant closeAt, String status, Instant createdAt) {
    public static DropResponse from(DropResult r) {
        return new DropResponse(r.id(), r.productId(), r.title(), r.entryCapacity(), r.winnerCount(),
                r.openAt(), r.closeAt(), r.status(), r.createdAt());
    }
}
