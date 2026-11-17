package com.queuedrop.entry.application.port;

import java.time.Instant;

/** application 자체 DTO. common 의 EntryCreated 메시지로의 변환은 infrastructure 가 맡는다. */
public record EntryCreatedPayload(
        String eventId, long dropId, long userId, int entrySeq, Instant appliedAt) {}
