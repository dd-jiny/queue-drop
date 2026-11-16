package com.queuedrop.product.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** products 와 연관관계를 맺지 않고 productId 만 가진다 (지연 로딩·N+1 차단, FK 는 DB 에만). */
@Entity
@Table(name = "drops")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DropJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(nullable = false, length = 100)
    private String title;

    @Column(name = "entry_capacity", nullable = false)
    private int entryCapacity;

    @Column(name = "winner_count", nullable = false)
    private int winnerCount;

    @Column(name = "open_at", nullable = false)
    private Instant openAt;

    @Column(name = "close_at", nullable = false)
    private Instant closeAt;

    @Column(nullable = false)
    private boolean drawn;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    DropJpaEntity(Long id, Long productId, String title, int entryCapacity, int winnerCount,
                  Instant openAt, Instant closeAt, boolean drawn, Instant createdAt) {
        this.id = id;
        this.productId = productId;
        this.title = title;
        this.entryCapacity = entryCapacity;
        this.winnerCount = winnerCount;
        this.openAt = openAt;
        this.closeAt = closeAt;
        this.drawn = drawn;
        this.createdAt = createdAt;
    }
}
