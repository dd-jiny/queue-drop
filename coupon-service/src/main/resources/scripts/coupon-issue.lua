-- KEYS[1] = coupon:{eventId}:meta
-- KEYS[2] = coupon:{eventId}:remaining
-- KEYS[3] = coupon:{eventId}:issued
-- KEYS[4] = coupon:{eventId}:requests
-- ARGV[1] = userId
-- ARGV[2] = nowEpochMs (애플리케이션 시각, 비결정적 TIME 명령 사용 금지)
-- ARGV[3] = requestId (요청 ID, v1.4)

local startAt = redis.call('HGET', KEYS[1], 'issueStartAt')
if not startAt then
  return 'NOT_FOUND'
end
if tonumber(ARGV[2]) < tonumber(startAt) then
  return 'NOT_STARTED'
end
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
  return 'DUPLICATE'
end
local remaining = tonumber(redis.call('GET', KEYS[2]) or '0')
if remaining <= 0 then
  return 'SOLD_OUT'
end
redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[3])   -- 이 자리를 잡은 요청 기록 (보상·저장 확정이 대조)
return 'OK'
