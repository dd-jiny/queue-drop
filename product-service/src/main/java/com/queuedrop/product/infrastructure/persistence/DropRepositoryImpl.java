package com.queuedrop.product.infrastructure.persistence;

import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;
import com.queuedrop.product.domain.repository.DropRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
class DropRepositoryImpl implements DropRepository {

    /** openAt 순, 같으면 id 순 — 페이지 경계에서 중복·누락이 없도록 정렬을 완전히 고정한다. */
    private static final Sort ORDER = Sort.by("openAt").and(Sort.by("id"));

    private final DropJpaRepository jpa;
    private final DropMapper mapper;

    @Override
    public Drop save(Drop drop) {
        return mapper.toDomain(jpa.save(mapper.toEntity(drop)));
    }

    @Override
    public Optional<Drop> findById(Long id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public DropPage search(DropStatus status, Instant now, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, ORDER);
        Page<DropJpaEntity> result = switch (status) {
            case null -> jpa.findAll(pageable);
            case SCHEDULED -> jpa.findScheduled(now, pageable);
            case OPEN -> jpa.findOpen(now, pageable);
            case CLOSED -> jpa.findClosed(now, pageable);
            case DRAWN -> jpa.findByDrawnTrue(pageable);
        };
        return new DropPage(result.getContent().stream().map(mapper::toDomain).toList(),
                result.getTotalElements(), page, size);
    }
}
