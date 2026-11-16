package com.queuedrop.product.application.port;

import java.time.Instant;

public record DropCreatedPayload(Long dropId, Long productId, String title, int entryCapacity,
                                 int winnerCount, Instant openAt, Instant closeAt) {
}
