-- rank.lua : (dropId, userId) 를 처음 볼 때만 3개 보드에 1점씩 더한다 (phase-7-ranking.md 3.3)
-- KEYS[1]=rank:dedupe:{dropId}, KEYS[2]=rank:drop:popular, KEYS[3]=rank:user:weekly:{yyyy-Www},
-- KEYS[4]=rank:user:all, KEYS[5]=rank:drop:closeAt
-- ARGV[1]=dropId, ARGV[2]=userId, ARGV[3]=주간 키 TTL(초), ARGV[4]=dedupe 유예(ms),
-- ARGV[5]=dedupe 기본 TTL(ms), ARGV[6]=dedupe 최소 TTL(ms), ARGV[7]=현재 시각(epoch ms)
-- 반환: 1 = 집계함, 0 = 중복(스킵)
-- 읽기·계산을 모두 끝낸 뒤 마지막에 쓴다: 계산 중 런타임 오류가 나도 dedupe 만 남고 점수가 빠지는 일이 없다
local dropId = ARGV[1]
local userId = ARGV[2]

-- 1) 멱등 처리: 이미 집계한 응모면 아무것도 하지 않는다
if redis.call('SISMEMBER', KEYS[1], userId) == 1 then
    return 0
end

-- 2) 계산 (쓰기 없음)
local now = tonumber(ARGV[7])
local weeklyTtl = tonumber(ARGV[3])
--    dedupe Set 만료 시각: closeAt + 유예. 단, 최소 TTL 보장
--    (재처리 시 closeAt 이 이미 지났더라도 키가 즉시 삭제되지 않도록). 키가 없으면(-2) 처음 만드는 것
local dedupeExpireAt = nil
if redis.call('PTTL', KEYS[1]) < 0 then
    local closeAt = redis.call('HGET', KEYS[5], dropId)
    if closeAt then
        dedupeExpireAt = tonumber(closeAt) + tonumber(ARGV[4])
    else
        dedupeExpireAt = now + tonumber(ARGV[5])
    end
    local minExpireAt = now + tonumber(ARGV[6])
    if dedupeExpireAt < minExpireAt then dedupeExpireAt = minExpireAt end
end
--    주간 키 TTL 은 처음 만들 때(-2) 또는 TTL 이 없을 때(-1)만 설정 — 연장하지 않는다
local weeklyNeedsTtl = redis.call('TTL', KEYS[3]) < 0

-- 3) 쓰기 (모두 끝에)
redis.call('SADD', KEYS[1], userId)
if dedupeExpireAt then redis.call('PEXPIREAT', KEYS[1], dedupeExpireAt) end
redis.call('ZINCRBY', KEYS[2], 1, dropId)
redis.call('ZINCRBY', KEYS[3], 1, userId)
if weeklyNeedsTtl then redis.call('EXPIRE', KEYS[3], weeklyTtl) end
redis.call('ZINCRBY', KEYS[4], 1, userId)

return 1
