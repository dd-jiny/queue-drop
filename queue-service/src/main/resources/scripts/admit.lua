-- 입장 (원자: 잔여 정원 확인 + ZPOPMIN + 토큰 발급). phase-3 3.3
-- KEYS[1] = queue:{dropId}:waiting
-- KEYS[2] = entry:{dropId}:remaining   (entry 소유, 읽기만)
-- ARGV[1] = n (배치 크기, 1~1000)
-- ARGV[2] = 토큰 TTL (초)
-- ARGV[3] = 토큰 키 prefix "queue:{dropId}:token:"
-- ARGV[4] = 발급 시각 epoch ms (토큰 값)
-- return: 입장한 userId 배열 (진입 순서)

local remaining = redis.call('GET', KEYS[2])
if not remaining or tonumber(remaining) <= 0 then
    return {}
end

local popped = redis.call('ZPOPMIN', KEYS[1], tonumber(ARGV[1]))
local admitted = {}
for i = 1, #popped, 2 do
    local userId = popped[i]
    redis.call('SET', ARGV[3] .. userId, ARGV[4], 'EX', tonumber(ARGV[2]))
    admitted[#admitted + 1] = userId
end
return admitted
