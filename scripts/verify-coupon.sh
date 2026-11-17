#!/usr/bin/env bash
# 쿠폰 발급 정합성 검증 (phase-8 3.5, 4장 T8-08·T8-09)
#   NFR-01 총 수량 초과 없음 / NFR-02 사용자 중복 없음 / NFR-03 Redis ↔ DB 일치
#
# 사용: scripts/verify-coupon.sh <couponEventId> [--wait-sec 60] [--expect N]
#   --wait-sec N  컨슈머 지연 해소 대기 시간(초). 기본 60
#   --expect N    기대 발급 수. 주면 "DB 발급 수 == N" 을 검사한다
#                 (S3: k6 coupon_ok, S4 dup_burst: DUP_USERS — T8-09 는 --expect 200)
# 종료 코드: 0 전부 PASS / 1 FAIL 하나 이상 또는 조회 불가 / 2 인자 오류
# 쿠폰 Redis 키(coupon:{id}:*)는 만료가 없어, 키가 없으면 초기화 누락·Redis 유실로 보고 FAIL 이다
set -euo pipefail
trap 'echo "FAIL  조회 실패 (line $LINENO: $BASH_COMMAND)"; exit 1' ERR

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() { sed -n '5,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2; }
is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }
is_int()  { [[ "${1:-}" =~ ^-?[0-9]+$ ]]; }

[[ $# -ge 1 ]] || usage
EVENT_ID=$1; shift
is_uint "$EVENT_ID" || { echo "[verify-coupon] couponEventId 는 숫자여야 합니다: $EVENT_ID" >&2; usage; }
WAIT_SEC=60; EXPECT=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --wait-sec) [[ $# -ge 2 ]] && is_uint "$2" || usage; WAIT_SEC=$2; shift ;;
    --expect)   [[ $# -ge 2 ]] && is_uint "$2" || usage; EXPECT=$2; shift ;;
    -h|--help)  usage ;;
    *) echo "[verify-coupon] 알 수 없는 인자: $1" >&2; usage ;;
  esac
  shift
done

sql()   { docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U queuedrop -d coupon_db -At -c "$1"; }
# exec -T 는 TTY 가 없어 redis-cli 가 raw 출력을 쓴다 (nil → 빈 줄)
redis() { docker compose exec -T redis redis-cli "$@" | tr -d '\r'; }
FAIL=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1${2:+  ($2)}"; FAIL=1; }
# 정수 비교: 두 값이 모두 정수 문자열일 때만 비교하고, 아니면 FAIL (빈 값을 0 으로 보지 않는다)
cmp() {   # cmp <이름> <a> <op: -eq|-le> <b> [상세]
  if ! is_int "$2" || ! is_int "$4"; then fail "$1" "정수 아님: '$2' $3 '$4'"; return 0; fi
  local ok=false
  case $3 in
    -eq) if [[ "$2" -eq "$4" ]]; then ok=true; fi ;;
    -le) if [[ "$2" -le "$4" ]]; then ok=true; fi ;;
  esac
  if $ok; then pass "$1"; else fail "$1" "${5:-$2 $3 $4}"; fi
}

if ! sql "SELECT 1" >/dev/null 2>&1; then echo "FAIL  coupon_db 조회 불가 (docker compose postgres 확인)"; exit 1; fi
if [[ "$(redis PING 2>/dev/null || true)" != "PONG" ]]; then echo "FAIL  Redis 조회 불가 (docker compose redis 확인)"; exit 1; fi

TOTAL=$(sql "SELECT total_quantity FROM coupon_events WHERE id=$EVENT_ID")
if [[ -z "$TOTAL" ]]; then
  echo "FAIL  coupon_events 에 id=$EVENT_ID 없음 (잘못된 couponEventId)"
  exit 1
fi

db_count() { sql "SELECT COUNT(*) FROM issued_coupons WHERE coupon_event_id=$EVENT_ID"; }
# 쿠폰 키를 한 번에(원자적으로) 읽는다: meta 존재, 잔여(없으면 '-'), 발급 수, 저장 확정 수
COUPON_LUA="return redis.call('EXISTS', KEYS[1]) .. ' ' .. (redis.call('GET', KEYS[2]) or '-') .. ' ' .. redis.call('SCARD', KEYS[3]) .. ' ' .. redis.call('SCARD', KEYS[4])"
read_coupon_keys() {
  read -r META_EXISTS REMAINING ISSUED PERSISTED <<<"$(redis EVAL "$COUPON_LUA" 4 \
    "coupon:$EVENT_ID:meta" "coupon:$EVENT_ID:remaining" "coupon:$EVENT_ID:issued" "coupon:$EVENT_ID:persisted")"
}

read_coupon_keys
if [[ "$META_EXISTS" != "1" ]]; then
  DB_COUNT=$(db_count)
  echo "eventId=$EVENT_ID total=$TOTAL db=$DB_COUNT"
  fail "NFR-03 Redis 쿠폰 키 존재" "coupon:$EVENT_ID:meta 없음 — 초기화 누락 또는 Redis 데이터 유실 (쿠폰 키는 만료 없음)"
  echo "결과: FAIL"
  exit 1
fi

# 컨슈머 지연 해소 대기: DB 발급 수가 Redis 발급 수에 도달할 때까지
DB_COUNT=$(db_count)
for ((s = 0; s < WAIT_SEC; s++)); do
  [[ "$DB_COUNT" == "$ISSUED" && "$DB_COUNT" == "$PERSISTED" ]] && break
  sleep 1
  read_coupon_keys
  DB_COUNT=$(db_count)
done
# DUP 은 DB UNIQUE 제약(coupon_event_id,user_id) 때문에 항상 0 (회귀 감시용으로 유지). 의미 있는 검사는 건수 비교다.
DUP=$(sql "SELECT COUNT(*) FROM (SELECT user_id FROM issued_coupons WHERE coupon_event_id=$EVENT_ID GROUP BY user_id HAVING COUNT(*)>1) t")

echo "eventId=$EVENT_ID total=$TOTAL remaining=$REMAINING redisIssued=$ISSUED persisted=$PERSISTED db=$DB_COUNT${EXPECT:+ expect=$EXPECT}"
cmp "NFR-01 DB 발급 수 ≤ 총 수량"     "$DB_COUNT" -le "$TOTAL"
cmp "NFR-01 Redis 발급 수 ≤ 총 수량"  "$ISSUED" -le "$TOTAL"
cmp "NFR-02 사용자 중복 0"            "$DUP" -eq 0
if [[ "$REMAINING" == "-" ]]; then
  fail "NFR-03 Redis 잔여 키 존재" "coupon:$EVENT_ID:remaining 없음"
elif ! is_int "$REMAINING"; then
  fail "NFR-03 Redis 잔여" "정수 아님: '$REMAINING'"
else
  cmp "NFR-01 Redis 잔여 ≥ 0"                 0 -le "$REMAINING"
  cmp "NFR-03 총 수량 - 잔여 == Redis 발급 수" $((TOTAL - REMAINING)) -eq "$ISSUED" "$TOTAL-$REMAINING != $ISSUED"
fi
cmp "NFR-03 Redis 발급 수 == DB 발급 수"            "$ISSUED" -eq "$DB_COUNT" "redis=$ISSUED db=$DB_COUNT (${WAIT_SEC}s 대기 후)"
cmp "NFR-03 저장 확정(persisted) 수 == DB 발급 수"  "$PERSISTED" -eq "$DB_COUNT" "persisted=$PERSISTED db=$DB_COUNT"
if [[ -n "$EXPECT" ]]; then
  cmp "기대 발급 수 == DB 발급 수"                   "$DB_COUNT" -eq "$EXPECT" "expect=$EXPECT db=$DB_COUNT"
fi

if ((FAIL)); then echo "결과: FAIL"; else echo "결과: PASS"; fi
exit $FAIL
