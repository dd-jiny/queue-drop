package com.queuedrop.entry.application.dto;

import java.time.Instant;

public record DrawCompletedPayload(Long dropId, int applicantCount, int winnerCount, Instant drawnAt) {}
