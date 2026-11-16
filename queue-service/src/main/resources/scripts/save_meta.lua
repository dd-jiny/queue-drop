-- drop.created 메타 저장 (원자: HSET + PEXPIREAT + 활성 인덱스 ZADD). phase-3 v1.3
-- KEYS[1] = queue:{dropId}:meta
-- KEYS[2] = queue:active-drops
-- ARGV[1] = dropId
-- ARGV[2] = openAt epoch ms
-- ARGV[3] = closeAt epoch ms
-- ARGV[4] = meta 만료 시각 epoch ms (closeAt + 보존 기간). 이미 지났으면 PEXPIREAT 이 키를 즉시 삭제한다
-- return: 1

redis.call('HSET', KEYS[1], 'openAt', ARGV[2], 'closeAt', ARGV[3])
redis.call('PEXPIREAT', KEYS[1], ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1
