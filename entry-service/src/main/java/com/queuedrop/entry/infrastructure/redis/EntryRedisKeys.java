package com.queuedrop.entry.infrastructure.redis;

/** entry-service Redis 키 이름의 단일 출처 (architecture.md 5장). queue-service 의 QueueKeys 와 같은 형태 */
public final class EntryRedisKeys {

    private EntryRedisKeys() {}

    public static String meta(Long dropId)       { return "entry:" + dropId + ":meta"; }
    /** queue-service 가 읽는다 (QueueKeys.entryRemaining). 값은 항상 10진 정수 문자열 */
    public static String remaining(Long dropId)  { return "entry:" + dropId + ":remaining"; }
    public static String applicants(Long dropId) { return "entry:" + dropId + ":applicants"; }
    public static String seq(Long dropId)        { return "entry:" + dropId + ":seq"; }

    /** queue-service 소유 키 — 읽기 전용. QueueKeys.token(dropId, userId) 과 같은 문자열이어야 한다 */
    public static String queueToken(Long dropId, Long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }
}
