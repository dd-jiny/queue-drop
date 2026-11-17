package com.queuedrop.entry.infrastructure.redis;

import java.util.List;

/** entry-service Redis 키 이름의 단일 출처 (architecture.md 5장). queue-service 의 QueueKeys 와 같은 형태 */
public final class EntryRedisKeys {

    private EntryRedisKeys() {}

    public static String meta(Long dropId)       { return "entry:" + dropId + ":meta"; }
    /** queue-service 가 읽는다 (QueueKeys.entryRemaining). 값은 항상 10진 정수 문자열 */
    public static String remaining(Long dropId)  { return "entry:" + dropId + ":remaining"; }
    public static String applicants(Long dropId) { return "entry:" + dropId + ":applicants"; }
    public static String seq(Long dropId)        { return "entry:" + dropId + ":seq"; }
    /** userId → 이 자리를 잡은 요청의 eventId (v1.5, E-40) */
    public static String requests(Long dropId)   { return "entry:" + dropId + ":requests"; }
    /** 저장 컨슈머가 DB 저장 전에 확정한 userId (v1.5, E-40) */
    public static String persisted(Long dropId)  { return "entry:" + dropId + ":persisted"; }

    /** entry 소유 응모 키 6개 — 추첨 후 만료 대상 (E-42). queue 토큰 키는 포함하지 않는다 */
    public static List<String> all(Long dropId) {
        return List.of(meta(dropId), remaining(dropId), applicants(dropId),
                seq(dropId), requests(dropId), persisted(dropId));
    }

    /** queue-service 소유 키 — 읽기 전용. QueueKeys.token(dropId, userId) 과 같은 문자열이어야 한다 */
    public static String queueToken(Long dropId, Long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }
}
