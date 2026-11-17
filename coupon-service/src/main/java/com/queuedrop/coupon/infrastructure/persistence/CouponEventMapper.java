package com.queuedrop.coupon.infrastructure.persistence;

import com.queuedrop.coupon.domain.model.CouponEvent;

final class CouponEventMapper {

    private CouponEventMapper() {
    }

    static CouponEventJpaEntity toEntity(CouponEvent event) {
        return new CouponEventJpaEntity(event.getId(), event.getName(), event.getTotalQuantity(),
                event.getIssueStartAt(), event.getCreatedAt());
    }

    static CouponEvent toDomain(CouponEventJpaEntity e) {
        return CouponEvent.restore(e.getId(), e.getName(), e.getTotalQuantity(),
                e.getIssueStartAt(), e.getCreatedAt());
    }
}
