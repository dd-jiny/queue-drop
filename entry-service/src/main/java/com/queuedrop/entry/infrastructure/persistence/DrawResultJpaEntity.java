package com.queuedrop.entry.infrastructure.persistence;

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

/** 유니크 제약(drop_id)과 CHECK 제약은 Flyway SQL 이 정의한다. */
@Entity
@Table(name = "draw_results")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class DrawResultJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "drop_id", nullable = false)
    private long dropId;

    @Column(nullable = false)
    private long seed;

    @Column(name = "applicant_count", nullable = false)
    private int applicantCount;

    @Column(name = "winner_count", nullable = false)
    private int winnerCount;

    @Column(name = "drawn_at", nullable = false)
    private Instant drawnAt;

    @Column(nullable = false)
    private boolean published;

    DrawResultJpaEntity(Long id, long dropId, long seed, int applicantCount, int winnerCount,
                        Instant drawnAt, boolean published) {
        this.id = id;
        this.dropId = dropId;
        this.seed = seed;
        this.applicantCount = applicantCount;
        this.winnerCount = winnerCount;
        this.drawnAt = drawnAt;
        this.published = published;
    }
}
