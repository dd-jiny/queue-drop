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

/** CHECK 제약(total_quantity >= 1)은 Flyway SQL 이 정의한다. */
@Entity
@Table(name = "coupon_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class CouponEventJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String name;

    @Column(name = "total_quantity", nullable = false)
    private int totalQuantity;

    @Column(name = "issue_start_at", nullable = false)
    private Instant issueStartAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    CouponEventJpaEntity(Long id, String name, int totalQuantity, Instant issueStartAt, Instant createdAt) {
        this.id = id;
        this.name = name;
        this.totalQuantity = totalQuantity;
        this.issueStartAt = issueStartAt;
        this.createdAt = createdAt;
    }
}
