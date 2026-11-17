package com.queuedrop.coupon.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

interface CouponEventJpaRepository extends JpaRepository<CouponEventJpaEntity, Long> {
}
