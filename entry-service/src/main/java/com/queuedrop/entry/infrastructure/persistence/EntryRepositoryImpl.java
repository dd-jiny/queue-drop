package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
class EntryRepositoryImpl implements EntryRepository {

    private final EntryJpaRepository jpa;

    @Override
    @Transactional
    public boolean saveIfAbsent(Entry entry) {
        int inserted = jpa.insertIfAbsent(entry.getDropId(), entry.getUserId(), entry.getEntrySeq(),
                entry.getStatus().name(), entry.getAppliedAt());
        return inserted == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return jpa.findByDropIdAndUserId(dropId, userId).map(EntryMapper::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Entry> findAllByUserId(long userId) {
        return jpa.findAllByUserIdOrderByAppliedAtDesc(userId).stream().map(EntryMapper::toDomain).toList();
    }
}
