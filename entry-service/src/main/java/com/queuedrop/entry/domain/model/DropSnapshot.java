package com.queuedrop.entry.domain.model;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Duration;
import java.time.Instant;
import lombok.Getter;

/** product 드로우의 사본. 생성 시 불변식을 검증한다. */
@Getter
public final class DropSnapshot {

    private final Long dropId;
    private final int entryCapacity;
    private final int winnerCount;
    private final Instant openAt;
    private final Instant closeAt;
    private final SnapshotStatus status;

    private DropSnapshot(Long dropId, int entryCapacity, int winnerCount,
                         Instant openAt, Instant closeAt, SnapshotStatus status) {
        this.dropId = dropId;
        this.entryCapacity = entryCapacity;
        this.winnerCount = winnerCount;
        this.openAt = openAt;
        this.closeAt = closeAt;
        this.status = status;
    }

    /** drop.created 수신 시 생성. 항상 OPEN 으로 시작한다. */
    public static DropSnapshot register(Long dropId, int entryCapacity, int winnerCount,
                                        Instant openAt, Instant closeAt) {
        if (dropId == null || openAt == null || closeAt == null) {
            throw new DomainException("필수 값 누락: dropId=" + dropId + ", openAt=" + openAt + ", closeAt=" + closeAt);
        }
        if (winnerCount < 1 || entryCapacity < winnerCount) {
            throw new DomainException("entryCapacity >= winnerCount >= 1 이어야 합니다: entryCapacity="
                    + entryCapacity + ", winnerCount=" + winnerCount);
        }
        if (!openAt.isBefore(closeAt)) {
            throw new DomainException("openAt < closeAt 이어야 합니다: openAt=" + openAt + ", closeAt=" + closeAt);
        }
        return new DropSnapshot(dropId, entryCapacity, winnerCount, openAt, closeAt, SnapshotStatus.OPEN);
    }

    /** 저장소에서 복원할 때만 사용 (Mapper 전용). 검증하지 않는다. */
    public static DropSnapshot restore(Long dropId, int entryCapacity, int winnerCount,
                                       Instant openAt, Instant closeAt, SnapshotStatus status) {
        return new DropSnapshot(dropId, entryCapacity, winnerCount, openAt, closeAt, status);
    }

    /** OPEN 이고 closeAt <= now 이면 마감 대상. */
    public boolean isDue(Instant now) {
        return status == SnapshotStatus.OPEN && !now.isBefore(closeAt);
    }

    /** CLOSING 이 closeAt + threshold 를 넘겨 이어지면 장기화. */
    public boolean isClosingTooLong(Instant now, Duration threshold) {
        return status == SnapshotStatus.CLOSING && now.isAfter(closeAt.plus(threshold));
    }

    public boolean isClosing() {
        return status == SnapshotStatus.CLOSING;
    }

    public boolean isDrawn() {
        return status == SnapshotStatus.DRAWN;
    }
}
