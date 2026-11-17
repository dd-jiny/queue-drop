package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
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
}
