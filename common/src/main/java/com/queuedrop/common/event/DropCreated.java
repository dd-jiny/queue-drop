package com.queuedrop.common.event;

import java.time.Instant;

/** product-service → entry, queue, ranking. key = dropId */
public record DropCreated(
        Long dropId,
        Long productId,
        String title,
        int entryCapacity,
        int winnerCount,
        Instant openAt,
        Instant closeAt
) {}
