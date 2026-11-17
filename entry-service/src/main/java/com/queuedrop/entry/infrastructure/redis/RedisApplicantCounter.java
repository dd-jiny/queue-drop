package com.queuedrop.entry.infrastructure.redis;

import com.queuedrop.entry.application.port.ApplicantCounter;
import com.queuedrop.entry.application.port.GateUnavailableException;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** ApplicantCounter 의 Redis 구현 (SCARD entry:{dropId}:applicants) */
@Component
@RequiredArgsConstructor
public class RedisApplicantCounter implements ApplicantCounter {

    private final StringRedisTemplate redis;

    @Override
    public long count(long dropId) {
        Long size;
        try {
            size = redis.opsForSet().size(EntryRedisKeys.applicants(dropId));
        } catch (DataAccessException e) {
            throw new GateUnavailableException("Redis 응모자 수 조회 실패", e);
        }
        return size == null ? 0L : size;
    }
}
