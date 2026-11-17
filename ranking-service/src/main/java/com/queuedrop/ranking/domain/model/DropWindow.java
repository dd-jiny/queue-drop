package com.queuedrop.ranking.domain.model;

import java.time.Instant;

/** 드로우 진행 여부 판단. closeAt 이 null 이면 아직 모름(drop.created 미수신). */
public record DropWindow(long dropId, Instant closeAt) {

    /** closeAt 을 아직 모르면(drop.created 미수신) 진행 중으로 간주한다. */
    public boolean isInProgress(Instant now) {
        return closeAt == null || now.isBefore(closeAt);
    }

    /** closeAt 을 알고 이미 지났을 때만 true. 인기 보드에서 lazy 제거 대상 */
    public boolean isClosed(Instant now) {
        return !isInProgress(now);
    }
}
