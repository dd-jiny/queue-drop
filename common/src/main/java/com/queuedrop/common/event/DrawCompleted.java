package com.queuedrop.common.event;

import java.time.Instant;

/** entry-service → product. key = dropId */
public record DrawCompleted(Long dropId, int applicantCount, int winnerCount, Instant drawnAt) {}
