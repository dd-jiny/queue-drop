package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.SnapshotStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** drop.created 로 받은 드로우 사본. id 는 product 의 drop id 를 그대로 쓴다 (자동 생성 아님). */
@Entity
@Table(name = "drop_snapshots")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class DropSnapshotJpaEntity {

    @Id
    @Column(name = "drop_id")
    private Long dropId;

    @Column(name = "entry_capacity", nullable = false)
    private int entryCapacity;

    @Column(name = "winner_count", nullable = false)
    private int winnerCount;

    @Column(name = "open_at", nullable = false)
    private Instant openAt;

    @Column(name = "close_at", nullable = false)
    private Instant closeAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private SnapshotStatus status;

    DropSnapshotJpaEntity(Long dropId, int entryCapacity, int winnerCount,
                          Instant openAt, Instant closeAt, SnapshotStatus status) {
        this.dropId = dropId;
        this.entryCapacity = entryCapacity;
        this.winnerCount = winnerCount;
        this.openAt = openAt;
        this.closeAt = closeAt;
        this.status = status;
    }
}
