package com.queuedrop.coupon.domain.model;

import com.queuedrop.coupon.domain.exception.DomainException;
import java.time.Instant;
import lombok.Getter;

/** 쿠폰 이벤트. 시작 시각(issueStartAt)부터 선착순 발급한다. */
@Getter
public final class CouponEvent {

    private final Long id;             // 저장 전 null
    private final String name;
    private final int totalQuantity;
    private final Instant issueStartAt;
    private final Instant createdAt;

    private CouponEvent(Long id, String name, int totalQuantity, Instant issueStartAt, Instant createdAt) {
        this.id = id;
        this.name = name;
        this.totalQuantity = totalQuantity;
        this.issueStartAt = issueStartAt;
        this.createdAt = createdAt;
    }

    /** issueStartAt 은 미래여야 한다 — 등록 직후 Redis 초기화 전에 발급이 시작되는 것을 막는다. */
    public static CouponEvent create(String name, int totalQuantity, Instant issueStartAt, Instant now) {
        if (issueStartAt == null) {
            throw new DomainException("issueStartAt is required");
        }
        if (name == null || name.isBlank()) {
            throw new DomainException("name is required");
        }
        if (totalQuantity < 1) {
            throw new DomainException("totalQuantity must be >= 1: " + totalQuantity);
        }
        if (!issueStartAt.isAfter(now)) {
            throw new DomainException("issueStartAt must be in the future: issueStartAt=" + issueStartAt
                    + ", now=" + now);
        }
        return new CouponEvent(null, name, totalQuantity, issueStartAt, now);
    }

    /** 저장소 복원용. 저장 당시 검증을 통과한 값이므로 다시 검증하지 않는다. */
    public static CouponEvent restore(Long id, String name, int totalQuantity, Instant issueStartAt,
                                      Instant createdAt) {
        return new CouponEvent(id, name, totalQuantity, issueStartAt, createdAt);
    }

    public boolean isStarted(Instant now) {
        return !now.isBefore(issueStartAt);
    }
}
