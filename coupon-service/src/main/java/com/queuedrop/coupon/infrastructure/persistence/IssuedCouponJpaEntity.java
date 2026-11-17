package com.queuedrop.coupon.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** 유니크 제약(coupon_event_id, user_id)과 FK 는 Flyway SQL 이 정의한다. 저장은 native INSERT 로 한다. */
@Entity
@Table(name = "issued_coupons")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class IssuedCouponJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "coupon_event_id", nullable = false)
    private Long couponEventId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "issued_at", nullable = false)
    private Instant issuedAt;
}
