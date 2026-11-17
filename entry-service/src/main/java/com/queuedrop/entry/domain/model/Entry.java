package com.queuedrop.entry.domain.model;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Instant;
import java.util.Objects;
import lombok.Getter;

/** 응모 한 건. 4단계는 생성만 하고 상태 변경(win, lose)은 5단계에서 추가한다. */
@Getter
public final class Entry {

    private final Long id;            // 저장 전 null
    private final long dropId;
    private final long userId;
    private final int entrySeq;
    private final EntryStatus status;
    private final Instant appliedAt;

    private Entry(Long id, long dropId, long userId, int entrySeq,
                  EntryStatus status, Instant appliedAt) {
        this.id = id;
        this.dropId = dropId;
        this.userId = userId;
        this.entrySeq = entrySeq;
        this.status = status;
        this.appliedAt = appliedAt;
    }

    /** Redis 판정을 통과한 응모를 기록용 모델로 만든다. */
    public static Entry apply(long dropId, long userId, int entrySeq, Instant appliedAt) {
        if (entrySeq < 1) {
            throw new DomainException("entrySeq 는 1 이상이어야 합니다: entrySeq=" + entrySeq);
        }
        Objects.requireNonNull(appliedAt, "appliedAt");
        return new Entry(null, dropId, userId, entrySeq, EntryStatus.APPLIED, appliedAt);
    }

    /** 저장소 복원 전용 (Mapper). */
    public static Entry restore(Long id, long dropId, long userId, int entrySeq,
                                EntryStatus status, Instant appliedAt) {
        return new Entry(id, dropId, userId, entrySeq, status, appliedAt);
    }
}
