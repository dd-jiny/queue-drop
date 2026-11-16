package com.queuedrop.queue.domain.model;

import com.queuedrop.queue.domain.exception.DomainException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * 드로우별 대기열 운영 시각. Redis 에 epoch ms 로 저장되므로 생성 시 밀리초로 내림한다
 * (DropCreated 는 µs 정밀도, 차이 &lt; 1ms).
 */
public record QueueMeta(Long dropId, Instant openAt, Instant closeAt) {

    public QueueMeta {
        if (dropId == null || openAt == null || closeAt == null) {
            throw new DomainException("필수 값 누락: dropId=" + dropId + ", openAt=" + openAt + ", closeAt=" + closeAt);
        }
        openAt = openAt.truncatedTo(ChronoUnit.MILLIS);
        closeAt = closeAt.truncatedTo(ChronoUnit.MILLIS);
        if (!openAt.isBefore(closeAt)) {
            throw new DomainException("openAt < closeAt 이어야 합니다: openAt=" + openAt + ", closeAt=" + closeAt);
        }
    }

    /** openAt <= now < closeAt */
    public boolean isOpen(Instant now) {
        return !now.isBefore(openAt) && now.isBefore(closeAt);
    }

    public boolean isBeforeOpen(Instant now) {
        return now.isBefore(openAt);
    }

    public boolean isClosed(Instant now) {
        return !now.isBefore(closeAt);
    }
}
