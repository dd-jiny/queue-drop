package com.queuedrop.product.domain.model;

import com.queuedrop.product.domain.exception.DomainException;
import lombok.Getter;

import java.time.Duration;
import java.time.Instant;

/**
 * 드로우. 생성 규칙(M ≥ K ≥ 1, openAt < closeAt, 오픈 1분 여유)과 시각 기반 상태 계산을 책임진다.
 * 응모 구간은 [openAt, closeAt) 반열린 구간이다.
 */
@Getter
public class Drop {

    /** 등록 시각 대비 오픈까지 최소 여유 (drop.created 전파 시간 확보) — drop-register.md 5.1 */
    public static final Duration MIN_OPEN_LEAD_TIME = Duration.ofMinutes(1);

    private final Long id;              // 저장 전 null
    private final Long productId;
    private final String title;
    private final int entryCapacity;    // M
    private final int winnerCount;      // K
    private final Instant openAt;
    private final Instant closeAt;
    private boolean drawn;
    private final Instant createdAt;

    private Drop(Long id, Long productId, String title, int entryCapacity, int winnerCount,
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

    public static Drop create(Long productId, String title, int entryCapacity, int winnerCount,
                              Instant openAt, Instant closeAt, Instant now) {
        if (productId == null) {
            throw new DomainException("상품 ID 는 필수입니다");
        }
        if (title == null || title.isBlank()) {
            throw new DomainException("드로우 제목은 필수입니다");
        }
        if (winnerCount < 1) {
            throw new DomainException("당첨 수량은 1 이상이어야 합니다");
        }
        if (entryCapacity < winnerCount) {
            throw new DomainException("응모 정원은 당첨 수량 이상이어야 합니다");
        }
        if (openAt == null || closeAt == null) {
            throw new DomainException("오픈·마감 시각은 필수입니다");
        }
        if (!openAt.isBefore(closeAt)) {
            throw new DomainException("오픈 시각은 마감 시각보다 앞서야 합니다");
        }
        if (openAt.isBefore(now.plus(MIN_OPEN_LEAD_TIME))) {
            throw new DomainException("오픈 시각은 현재로부터 최소 1분 이후여야 합니다");
        }
        return new Drop(null, productId, title, entryCapacity, winnerCount, openAt, closeAt, false, now);
    }

    /** 영속 계층 복원 전용 (Mapper 에서만 사용). 과거 시각도 그대로 복원하므로 검증하지 않는다. */
    public static Drop restore(Long id, Long productId, String title, int entryCapacity, int winnerCount,
                               Instant openAt, Instant closeAt, boolean drawn, Instant createdAt) {
        return new Drop(id, productId, title, entryCapacity, winnerCount, openAt, closeAt, drawn, createdAt);
    }

    /** 시각으로 상태를 계산한다. 저장 값 drawn 이 시각보다 우선한다. */
    public DropStatus status(Instant now) {
        if (drawn) {
            return DropStatus.DRAWN;
        }
        if (now.isBefore(openAt)) {
            return DropStatus.SCHEDULED;    // now < openAt
        }
        if (now.isBefore(closeAt)) {
            return DropStatus.OPEN;         // openAt <= now < closeAt
        }
        return DropStatus.CLOSED;           // closeAt <= now
    }

    /** 5단계 draw.completed 수신 시 호출. 멱등. */
    public void markDrawn() {
        this.drawn = true;
    }
}
