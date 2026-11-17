-- 저장 확정 (entry.created 저장 컨슈머, DB INSERT 전)
-- KEYS[1] = entry:{dropId}:applicants
-- KEYS[2] = entry:{dropId}:requests
-- KEYS[3] = entry:{dropId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = eventId (메시지의 eventId)
-- return  : 1 = 저장 확정 (재전달에도 1), 0 = 저장하지 않음 (유령 이벤트)

-- 1. 보상된 응모의 유령 이벤트
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
    return 0
end

-- 2. 보상 후 재응모한 사용자의 이전 시도 유령 이벤트
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 0
end

-- 3. 확정: 이후 보상은 ALREADY_PERSISTED 로 거절된다
redis.call('SADD', KEYS[3], ARGV[1])
return 1
