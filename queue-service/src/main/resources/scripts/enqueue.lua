-- 대기열 진입 (원자). phase-3 3.3
-- KEYS[1] = queue:{dropId}:waiting
-- KEYS[2] = queue:{dropId}:seq
-- KEYS[3] = queue:{dropId}:token:{userId}
-- KEYS[4] = queue:{dropId}:meta
-- ARGV[1] = userId
-- ARGV[2] = 키 보존 기간 ms (closeAt 이후)
-- return: -1 = 이미 토큰 보유(ADMITTED), 0 이상 = 0-based rank (내 앞 인원 수)

if redis.call('EXISTS', KEYS[3]) == 1 then
    return -1
end

local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
if rank then
    return rank
end

local seq = redis.call('INCR', KEYS[2])
redis.call('ZADD', KEYS[1], 'NX', seq, ARGV[1])

-- waiting·seq 는 closeAt + 보존 기간에 만료. ZPOPMIN 으로 비면 키가 사라지므로 신규 등록마다 다시 건다
local closeAt = redis.call('HGET', KEYS[4], 'closeAt')
if closeAt then
    local expireAt = tonumber(closeAt) + tonumber(ARGV[2])
    redis.call('PEXPIREAT', KEYS[1], expireAt)
    redis.call('PEXPIREAT', KEYS[2], expireAt)
end
return redis.call('ZRANK', KEYS[1], ARGV[1])
