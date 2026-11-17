package com.queuedrop.entry.application.dto;

import com.queuedrop.entry.domain.model.Entry;
import java.time.Instant;

/**
 * status 는 문자열로 내보낸다 (APPLIED / WON / LOST).
 * domain enum 을 그대로 넣으면 presentation 이 domain 타입에 의존하게 된다 (R4).
 * entrySeq, appliedAt 은 Redis 로 보완한 경우 null 이다.
 */
public record MyEntryResult(long dropId, Integer entrySeq, String status, Instant appliedAt) {

    public static MyEntryResult from(Entry entry) {
        return new MyEntryResult(entry.getDropId(), entry.getEntrySeq(),
                entry.getStatus().name(), entry.getAppliedAt());
    }

    public static MyEntryResult pendingSave(long dropId) {
        return new MyEntryResult(dropId, null, "APPLIED", null);
    }
}
