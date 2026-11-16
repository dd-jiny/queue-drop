package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.DropSnapshot;

final class DropSnapshotMapper {

    private DropSnapshotMapper() {
    }

    static DropSnapshot toDomain(DropSnapshotJpaEntity e) {
        return DropSnapshot.restore(e.getDropId(), e.getEntryCapacity(), e.getWinnerCount(),
                e.getOpenAt(), e.getCloseAt(), e.getStatus());
    }

    static DropSnapshotJpaEntity toEntity(DropSnapshot s) {
        return new DropSnapshotJpaEntity(s.getDropId(), s.getEntryCapacity(), s.getWinnerCount(),
                s.getOpenAt(), s.getCloseAt(), s.getStatus());
    }
}
