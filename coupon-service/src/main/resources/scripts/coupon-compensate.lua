-- KEYS[1] = coupon:{eventId}:remaining
-- KEYS[2] = coupon:{eventId}:issued
-- KEYS[3] = coupon:{eventId}:requests
-- KEYS[4] = coupon:{eventId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = requestId (되돌릴 요청)
-- return  : 1 = COMPENSATED, 0 = NOT_OURS, 2 = ALREADY_PERSISTED
if redis.call('SISMEMBER', KEYS[4], ARGV[1]) == 1 then
  return 2                                         -- 저장 확정됨: 되돌리지 않는다
end
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0                                         -- 이 요청의 발급이 아님 (판정 미실행, 이미 보상, 다른 요청)
end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('INCR', KEYS[1])
return 1
