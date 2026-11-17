-- 응모 판정 되돌리기 (Kafka 발행 최종 실패, 또는 판정 Lua 응답 타임아웃 시)
-- KEYS[1] = entry:{dropId}:remaining
-- KEYS[2] = entry:{dropId}:applicants
-- KEYS[3] = entry:{dropId}:requests
-- KEYS[4] = entry:{dropId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = eventId (되돌릴 요청)
-- return  : 1 = COMPENSATED, 0 = NOT_OURS, 2 = ALREADY_PERSISTED

-- 1. 저장 컨슈머가 이미 확정했으면 되돌리지 않는다 (DB 저장 ⇔ Redis 응모자 유지)
if redis.call('SISMEMBER', KEYS[4], ARGV[1]) == 1 then
    return 2
end

-- 2. 이 요청이 잡은 자리가 아니면 건드리지 않는다 (다른 요청의 응모, 판정 미실행, 이미 보상됨)
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
    return 0
end

-- 3. 되돌림 (seq 는 되돌리지 않는다)
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('INCR', KEYS[1])
return 1
