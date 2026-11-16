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
import java.time.temporal.ChronoUnit;
import java.util.List;
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
        // pgjdbc 는 Instant 를 마이크로초로 반올림한다. 내림해 두어야 Drop.status(now) 와 경계가 같다 (저장 값은 마이크로초)
        Instant at = now.truncatedTo(ChronoUnit.MICROS);
        if ((long) page * size > Integer.MAX_VALUE) {
            // Spring Data 는 int 를 넘는 offset 에서 예외를 던진다. 범위 밖 페이지는 빈 결과로 답한다 (건수는 실제 값)
            long total = query(status, at, PageRequest.of(0, 1, ORDER)).getTotalElements();
            return new DropPage(List.of(), total, page, size);
        }
        Page<DropJpaEntity> result = query(status, at, PageRequest.of(page, size, ORDER));
        return new DropPage(result.getContent().stream().map(mapper::toDomain).toList(),
                result.getTotalElements(), page, size);
    }

    private Page<DropJpaEntity> query(DropStatus status, Instant now, Pageable pageable) {
        return switch (status) {
            case null -> jpa.findAll(pageable);
            case SCHEDULED -> jpa.findScheduled(now, pageable);
            case OPEN -> jpa.findOpen(now, pageable);
            case CLOSED -> jpa.findClosed(now, pageable);
            case DRAWN -> jpa.findByDrawnTrue(pageable);
        };
    }
}
