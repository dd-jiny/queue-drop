package com.queuedrop.product.application.dto;

import com.queuedrop.product.domain.model.Drop;

import java.time.Instant;

/** status 는 String 으로 노출한다 — presentation 이 domain enum 을 import 하지 않도록 (R4). */
public record DropResult(Long id, Long productId, String title, int entryCapacity, int winnerCount,
                         Instant openAt, Instant closeAt, String status, Instant createdAt) {
    public static DropResult of(Drop d, Instant now) {
        return new DropResult(d.getId(), d.getProductId(), d.getTitle(), d.getEntryCapacity(),
                d.getWinnerCount(), d.getOpenAt(), d.getCloseAt(), d.status(now).name(), d.getCreatedAt());
    }
}
