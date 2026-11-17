package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
class DropSnapshotRepositoryImpl implements DropSnapshotRepository {

    private final DropSnapshotJpaRepository jpa;

    @Override
    @Transactional
    public boolean saveIfAbsent(DropSnapshot snapshot) {
        return jpa.insertIfAbsent(DropSnapshotMapper.toEntity(snapshot)) == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<DropSnapshot> findById(long dropId) {
        return jpa.findById(dropId).map(DropSnapshotMapper::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Long> findDueOpenIds(Instant now) {
        return jpa.findDueOpenIds(now);
    }

    @Override
    @Transactional
    public boolean markClosingIfOpen(long dropId, Instant now) {
        return jpa.markClosingIfOpen(dropId, now) == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public List<DropSnapshot> findClosing() {
        return jpa.findByStatus(SnapshotStatus.CLOSING).stream().map(DropSnapshotMapper::toDomain).toList();
    }

    @Override
    @Transactional
    public boolean markDrawnIfClosing(long dropId) {
        return jpa.markDrawnIfClosing(dropId) == 1;
    }
}
