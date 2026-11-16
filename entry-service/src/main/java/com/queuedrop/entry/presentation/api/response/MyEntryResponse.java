package com.queuedrop.entry.presentation.api.response;

import com.queuedrop.entry.application.dto.MyEntryResult;
import java.time.Instant;

/** entrySeq, appliedAt 은 Redis 로 보완한 경우(DB 저장 대기) null 이다. */
public record MyEntryResponse(long dropId, Integer entrySeq, String status, Instant appliedAt) {

    public static MyEntryResponse from(MyEntryResult r) {
        return new MyEntryResponse(r.dropId(), r.entrySeq(), r.status(), r.appliedAt());
    }
}
