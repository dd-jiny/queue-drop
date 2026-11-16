package com.queuedrop.queue.infrastructure.redis.support;

import java.util.List;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/** Redis 통합 테스트 공통 도우미 */
public final class RedisTestSupport {

    private RedisTestSupport() {
    }

    public static void flushAll(StringRedisTemplate redis) {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    /**
     * PEXPIRETIME (Redis 7) 을 Lua 로 실행한다: 만료 시각 epoch ms. 만료 없음 -1, 키 없음 -2.
     * connection.execute(...) 는 Lettuce ByteArrayOutput 을 써서 정수 응답을 받지 못하므로 스크립트로 우회한다.
     */
    private static final RedisScript<Long> PEXPIRETIME =
            RedisScript.of("return redis.call('PEXPIRETIME', KEYS[1])", Long.class);

    public static long pexpireTime(StringRedisTemplate redis, String key) {
        Long value = redis.execute(PEXPIRETIME, List.of(key));
        return value == null ? Long.MIN_VALUE : value;
    }
}
