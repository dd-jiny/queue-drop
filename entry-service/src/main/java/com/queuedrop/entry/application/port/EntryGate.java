package com.queuedrop.entry.application.port;

import java.time.Instant;

/** Redis 기반 선착순 판정 게이트. 판정·보상·초기화를 원자적으로 수행한다. */
public interface EntryGate {

    GateResult tryApply(long dropId, long userId, Instant now);

    /** 판정을 되돌린다. 응모자에 없으면 아무것도 하지 않는다 (두 번 호출해도 안전). seq 는 되돌리지 않는다. */
    void compensate(long dropId, long userId);

    boolean isApplicant(long dropId, long userId);

    /** 응모 메타·정원 초기화. 이미 초기화돼 있으면 아무것도 하지 않는다. */
    void initialize(long dropId, int capacity, Instant openAt, Instant closeAt);
}
