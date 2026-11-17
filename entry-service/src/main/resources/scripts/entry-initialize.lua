-- 드로우 응모 상태 초기화. 이미 초기화된 드로우는 건드리지 않는다 (drop.created 재수신 대비).
-- KEYS[1] = entry:{dropId}:meta
-- KEYS[2] = entry:{dropId}:remaining
-- ARGV[1] = capacity
-- ARGV[2] = openAt (epoch ms)
-- ARGV[3] = closeAt (epoch ms)
-- return  : 1 = 초기화함, 0 = 이미 있음

if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

redis.call('HSET', KEYS[1],
    'state', 'OPEN',
    'capacity', ARGV[1],
    'openAt', ARGV[2],
    'closeAt', ARGV[3])
redis.call('SET', KEYS[2], ARGV[1])
return 1
