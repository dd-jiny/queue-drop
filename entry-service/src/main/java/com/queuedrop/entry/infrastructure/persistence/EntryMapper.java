package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.Entry;

/** 저장은 native INSERT 로 필드를 직접 넘기므로 toEntity 는 5단계(상태 변경)에서 필요해지면 추가한다. */
final class EntryMapper {

    private EntryMapper() {
    }

    static Entry toDomain(EntryJpaEntity e) {
        return Entry.restore(e.getId(), e.getDropId(), e.getUserId(),
                e.getEntrySeq(), e.getStatus(), e.getAppliedAt());
    }
}
