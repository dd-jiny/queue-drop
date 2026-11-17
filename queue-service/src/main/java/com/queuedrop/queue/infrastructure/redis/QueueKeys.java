package com.queuedrop.queue.infrastructure.redis;

/** queue-service Redis 키 이름의 단일 출처 (architecture.md 5장) */
public final class QueueKeys {

    public static final String ACTIVE_DROPS = "queue:active-drops";

    private QueueKeys() {}

    public static String meta(Long dropId) { return "queue:" + dropId + ":meta"; }
    public static String seq(Long dropId) { return "queue:" + dropId + ":seq"; }
    public static String waiting(Long dropId) { return "queue:" + dropId + ":waiting"; }
    public static String tokenPrefix(Long dropId) { return "queue:" + dropId + ":token:"; }
    public static String token(Long dropId, Long userId) { return tokenPrefix(dropId) + userId; }
    /** 스케줄러 게이트: queue:{name}:gate */
    public static String gate(String name) { return "queue:" + name + ":gate"; }

    /** entry-service 소유 — 읽기 전용 */
    public static String entryRemaining(Long dropId) { return "entry:" + dropId + ":remaining"; }
}
