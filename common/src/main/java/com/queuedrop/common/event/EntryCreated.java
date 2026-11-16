package com.queuedrop.common.event;

import java.time.Instant;

/** entry-service → entry(저장), ranking. key = "{dropId}:{userId}" */
public record EntryCreated(
        String eventId,      // UUID
        Long dropId,
        Long userId,
        int entrySeq,
        Instant appliedAt
) {}
