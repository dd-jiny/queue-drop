package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.DrawResult;

final class DrawResultMapper {

    private DrawResultMapper() {
    }

    static DrawResult toDomain(DrawResultJpaEntity e) {
        return DrawResult.restore(e.getId(), e.getDropId(), e.getSeed(), e.getApplicantCount(),
                e.getWinnerCount(), e.getDrawnAt(), e.isPublished());
    }

    static DrawResultJpaEntity toEntity(DrawResult r) {
        return new DrawResultJpaEntity(r.getId(), r.getDropId(), r.getSeed(), r.getApplicantCount(),
                r.getWinnerCount(), r.getDrawnAt(), r.isPublished());
    }
}
