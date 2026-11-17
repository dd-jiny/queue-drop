package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
class DrawResultRepositoryImpl implements DrawResultRepository {

    private final DrawResultJpaRepository jpa;

    /** drop_id 중복(uk_draw_results_drop_id)이 커밋 시점이 아니라 이 호출에서 드러나도록 즉시 flush 한다. */
    @Override
    @Transactional
    public DrawResult save(DrawResult result) {
        return DrawResultMapper.toDomain(jpa.saveAndFlush(DrawResultMapper.toEntity(result)));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<DrawResult> findByDropId(long dropId) {
        return jpa.findByDropId(dropId).map(DrawResultMapper::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<DrawResult> findUnpublished() {
        return jpa.findByPublishedFalse().stream().map(DrawResultMapper::toDomain).toList();
    }

    @Override
    @Transactional
    public void markPublished(long dropId) {
        jpa.markPublished(dropId);
    }
}
