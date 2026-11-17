package com.queuedrop.entry.domain.model;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Instant;
import java.util.Objects;
import lombok.Getter;

/** 추첨 기록. 시드를 남겨 결과를 재현할 수 있게 한다. */
@Getter
public final class DrawResult {

    private final Long id;             // 저장 전 null
    private final Long dropId;
    private final long seed;
    private final int applicantCount;
    private final int winnerCount;     // 실제 당첨 수 = min(K, 응모자 수)
    private final Instant drawnAt;
    private final boolean published;   // draw.completed 발행 여부

    private DrawResult(Long id, Long dropId, long seed, int applicantCount, int winnerCount,
                       Instant drawnAt, boolean published) {
        this.id = id;
        this.dropId = dropId;
        this.seed = seed;
        this.applicantCount = applicantCount;
        this.winnerCount = winnerCount;
        this.drawnAt = drawnAt;
        this.published = published;
    }

    public static DrawResult of(Long dropId, long seed, int applicantCount, int winnerCount, Instant drawnAt) {
        Objects.requireNonNull(dropId, "dropId");
        Objects.requireNonNull(drawnAt, "drawnAt");
        if (applicantCount < 0 || winnerCount < 0 || winnerCount > applicantCount) {
            throw new DomainException("applicantCount >= winnerCount >= 0 이어야 합니다: applicantCount="
                    + applicantCount + ", winnerCount=" + winnerCount);
        }
        return new DrawResult(null, dropId, seed, applicantCount, winnerCount, drawnAt, false);
    }

    /** published=true 인 새 인스턴스. 원본은 바뀌지 않는다. */
    public DrawResult markPublished() {
        return new DrawResult(id, dropId, seed, applicantCount, winnerCount, drawnAt, true);
    }
}
