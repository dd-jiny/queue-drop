package com.queuedrop.product.infrastructure.persistence;

import com.queuedrop.product.domain.model.Drop;
import org.springframework.stereotype.Component;

@Component
class DropMapper {

    Drop toDomain(DropJpaEntity e) {
        return Drop.restore(e.getId(), e.getProductId(), e.getTitle(), e.getEntryCapacity(), e.getWinnerCount(),
                e.getOpenAt(), e.getCloseAt(), e.isDrawn(), e.getCreatedAt());
    }

    DropJpaEntity toEntity(Drop d) {
        return new DropJpaEntity(d.getId(), d.getProductId(), d.getTitle(), d.getEntryCapacity(), d.getWinnerCount(),
                d.getOpenAt(), d.getCloseAt(), d.isDrawn(), d.getCreatedAt());
    }
}
