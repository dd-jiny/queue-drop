-- KEYS[1] = coupon:{eventId}:issued
-- KEYS[2] = coupon:{eventId}:requests
-- KEYS[3] = coupon:{eventId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = requestId (메시지의 eventId)
-- return  : 1 = 저장 확정 (재전달에도 1), 0 = 저장하지 않음 (유령 이벤트)
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return 0                                         -- 보상된 발급
end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0                                         -- 보상 후 재발급한 사용자의 이전 시도 유령 이벤트
end
redis.call('SADD', KEYS[3], ARGV[1])
return 1
