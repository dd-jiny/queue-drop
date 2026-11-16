package com.queuedrop.queue.infrastructure.redis.support;

import org.springframework.data.redis.core.StringRedisTemplate;

/** entry-service 가 없으므로 entry 소유 잔여 정원 키를 테스트가 대신 세팅한다 (단계 문서 3.5). */
public class EntryRemainingFixture {

    private final StringRedisTemplate redis;

    public EntryRemainingFixture(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** entry-service 가 drop.created 처리 시 하는 SET NX 를 흉내낸다 */
    public void initialize(Long dropId, long capacity) {
        redis.opsForValue().set(key(dropId), String.valueOf(capacity));
    }

    public void soldOut(Long dropId) {
        initialize(dropId, 0);
    }

    public void clear(Long dropId) {
        redis.delete(key(dropId));
    }

    /** 키 이름은 architecture.md 5장과 일치해야 한다. QueueKeys 와 독립적으로 고정한다 */
    private static String key(Long dropId) {
        return "entry:" + dropId + ":remaining";
    }
}
