package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.EntryStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** 유니크 제약((drop_id, user_id), (drop_id, entry_seq))은 Flyway SQL 이 정의한다. */
@Entity
@Table(name = "entries")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class EntryJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "drop_id", nullable = false)
    private long dropId;

    @Column(name = "user_id", nullable = false)
    private long userId;

    @Column(name = "entry_seq", nullable = false)
    private int entrySeq;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private EntryStatus status;

    @Column(name = "applied_at", nullable = false)
    private Instant appliedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    EntryJpaEntity(Long id, long dropId, long userId, int entrySeq,
                   EntryStatus status, Instant appliedAt, Instant updatedAt) {
        this.id = id;
        this.dropId = dropId;
        this.userId = userId;
        this.entrySeq = entrySeq;
        this.status = status;
        this.appliedAt = appliedAt;
        this.updatedAt = updatedAt;
    }
}
