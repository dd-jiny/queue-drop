package com.queuedrop.queue.infrastructure.redis;

import com.queuedrop.queue.application.port.RemainingCapacityReader;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** entry-service 소유 키 읽기 전용 (D3) */
@Component
@RequiredArgsConstructor
public class RedisRemainingCapacityReader implements RemainingCapacityReader {

    private final StringRedisTemplate redis;

    @Override
    public OptionalLong read(Long dropId) {
        String v = redis.opsForValue().get(QueueKeys.entryRemaining(dropId));
        return v == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(v));
    }
}
