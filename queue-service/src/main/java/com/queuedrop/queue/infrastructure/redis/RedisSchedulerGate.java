package com.queuedrop.queue.infrastructure.redis;

import com.queuedrop.queue.application.port.SchedulerGate;
import java.time.Duration;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 전역 레이트 게이트: SET queue:{name}:gate {instanceId} NX PX {window}.
 * 작업 후 해제(DEL)하지 않는다 — 해제하면 같은 주기 안에 다른 인스턴스가 다시 실행해 주기당 2N명이 입장할 수 있다.
 */
@Component
public class RedisSchedulerGate implements SchedulerGate {

    private final StringRedisTemplate redis;
    private final String instanceId;

    @Autowired
    public RedisSchedulerGate(StringRedisTemplate redis) {
        this(redis, UUID.randomUUID().toString());
    }

    /** 인스턴스 ID 를 지정한다 (다중 인스턴스 테스트용) */
    public RedisSchedulerGate(StringRedisTemplate redis, String instanceId) {
        this.redis = redis;
        this.instanceId = instanceId;
    }

    @Override
    public boolean tryPass(String name, Duration window) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(QueueKeys.gate(name), instanceId, window));
    }
}
