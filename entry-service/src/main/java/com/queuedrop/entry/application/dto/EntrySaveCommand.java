package com.queuedrop.entry.application.dto;

import java.time.Instant;

public record EntrySaveCommand(long dropId, long userId, int entrySeq, Instant appliedAt) {}
