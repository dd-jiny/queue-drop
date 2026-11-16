package com.queuedrop.common.test.archunit.sample.good.infrastructure.persistence;

import com.queuedrop.common.test.archunit.sample.good.application.port.DropRepository;
import com.queuedrop.common.test.archunit.sample.good.domain.model.Drop;
import java.util.List;
import java.util.Optional;

public class DropRepositoryAdapter implements DropRepository {
    @Override public Optional<Drop> findById(Long id) { return Optional.of(new DropEntity(id, 1).toDomain()); }
    @Override public List<Drop> findAll() { return List.of(); }
}
