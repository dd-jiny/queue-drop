-- 응모 판정 되돌리기 (Kafka 발행 최종 실패 시)
-- KEYS[1] = entry:{dropId}:remaining
-- KEYS[2] = entry:{dropId}:applicants
-- ARGV[1] = userId
-- return  : 1 = 되돌림, 0 = 응모자에 없음 (이미 보상됐거나 판정된 적 없음)

if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
    redis.call('INCR', KEYS[1])
    return 1
end
return 0
