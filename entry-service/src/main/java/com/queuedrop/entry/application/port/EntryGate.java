package com.queuedrop.entry.application.port;

import java.time.Instant;

/**
 * Redis 기반 선착순 판정 게이트. 판정·보상·저장 확정·초기화를 원자적으로 수행한다.
 * tryApply·compensate·claimForSave 는 Redis 장애(타임아웃 포함) 시 GateUnavailableException 을 던진다.
 */
public interface EntryGate {

    /** OK 면 requests[userId] = eventId 를 함께 기록한다. eventId 는 호출자가 판정 전에 만든 요청 ID */
    GateResult tryApply(long dropId, long userId, String eventId, Instant now);

    /**
     * eventId 요청의 판정을 되돌린다. seq 는 되돌리지 않는다. 두 번 호출해도 안전하다 (두 번째는 NOT_OURS).
     * @return ALREADY_PERSISTED: 저장 컨슈머가 이미 확정 — 되돌리지 않음
     *         NOT_OURS: requests[userId] ≠ eventId (다른 요청의 응모 또는 판정 미실행) — 되돌리지 않음
     *         COMPENSATED: 응모자·요청 제거 + 정원 복구
     */
    CompensationResult compensate(long dropId, long userId, String eventId);

    /**
     * 저장 컨슈머가 DB 저장 전에 호출한다. true 를 반환한 뒤에는 보상이 거절된다 (persisted).
     * @return false: 보상된 응모이거나 이전 시도의 유령 이벤트 — 저장하지 않는다. 재전달에도 true (멱등)
     */
    boolean claimForSave(long dropId, long userId, String eventId);

    /** 단건 조회의 저장 대기 보완용 (EntryQueryService). 저장 경로에서는 쓰지 않는다 */
    boolean isApplicant(long dropId, long userId);

    /** 응모 메타·정원 초기화. 이미 초기화돼 있으면 아무것도 하지 않는다. */
    void initialize(long dropId, int capacity, Instant openAt, Instant closeAt);
}
