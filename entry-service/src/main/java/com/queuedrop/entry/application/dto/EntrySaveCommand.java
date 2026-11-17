package com.queuedrop.entry.application.dto;

import java.time.Instant;

/** eventId: 메시지의 eventId = 응모 요청 ID */
public record EntrySaveCommand(String eventId, long dropId, long userId, int entrySeq, Instant appliedAt) {}
