-- 선착순 응모 원자 판정 (entry-apply.md 3장 표의 순서 그대로)
-- KEYS[1] = entry:{dropId}:meta
-- KEYS[2] = entry:{dropId}:remaining
-- KEYS[3] = entry:{dropId}:applicants
-- KEYS[4] = entry:{dropId}:seq
-- KEYS[5] = queue:{dropId}:token:{userId}     (queue-service 소유, 읽기만)
-- ARGV[1] = userId
-- ARGV[2] = now (epoch ms, 애플리케이션 시각)
-- return  : { 'OK', entrySeq } 또는 { 'CLOSED' | 'NO_TOKEN' | 'DUPLICATE' | 'SOLD_OUT' }

-- 1. 마감 여부: state == OPEN 이고 openAt <= now < closeAt
local meta = redis.call('HMGET', KEYS[1], 'state', 'openAt', 'closeAt')
if not meta[1] then
    return { 'CLOSED' }                      -- 초기화 전 드로우 (구현 결정)
end
local now = tonumber(ARGV[2])
if meta[1] ~= 'OPEN' or now < tonumber(meta[2]) or now >= tonumber(meta[3]) then
    return { 'CLOSED' }
end

-- 2. 입장 토큰
if redis.call('EXISTS', KEYS[5]) == 0 then
    return { 'NO_TOKEN' }
end

-- 3. 중복 응모
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
    return { 'DUPLICATE' }
end

-- 4. 잔여 정원
local remaining = tonumber(redis.call('GET', KEYS[2]) or '0')
if remaining <= 0 then
    return { 'SOLD_OUT' }
end

-- 5. 차감 + 응모자 등록
redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])

-- 6. 응모 번호: 잔여 정원과 무관한 카운터 (증가만, 보상 시에도 되돌리지 않음)
local seq = redis.call('INCR', KEYS[4])
return { 'OK', seq }
