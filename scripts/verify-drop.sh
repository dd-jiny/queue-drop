#!/usr/bin/env bash
# 드로우 응모 정합성 검증 (phase-8 3.5, 4장 T8-05·T8-06·T8-11)
#   NFR-01 정원 초과 없음 / NFR-02 중복 없음 / NFR-03 Redis ↔ DB 일치 / REQ-RANK-03 인기 랭킹 점수
#   --after-draw: 추첨 완료(DRAWN + draw_results)까지 기다린 뒤 추첨 결과 검증 (REQ-ENTRY-05)
#
# 사용: scripts/verify-drop.sh <dropId> [--after-draw] [--wait-sec 60] [--expect N] [--key-ttl-sec 86400]
#   --after-draw      추첨 결과까지 검증. DRAWN 이 될 때까지 --wait-sec 동안 폴링한다
#   --wait-sec N      컨슈머 지연 해소·랭킹 반영·추첨 완료 대기 시간(초, 각각 적용). 기본 60
#   --expect N        k6 요약의 entry_ok(성공 응답 수). 주면 "DB 응모 수 == N" 을 검사한다
#   --key-ttl-sec N   추첨 후 응모 키 TTL(초). entry.draw.key-ttl-after-draw 와 맞춘다. 기본 86400 (1d)
# 종료 코드: 0 전부 PASS(SKIP 포함) / 1 FAIL 하나 이상 또는 조회 실패 / 2 인자 오류
#
# SKIP 기준
#   - Redis 응모 키(entry:{dropId}:meta)가 없고 draw_results.drawn_at + key-ttl 이 지남: 추첨 후 TTL 만료.
#     그 전에 키가 없으면(추첨 전, 또는 추첨 후 TTL 미경과) Redis 유실·초기화 누락으로 보고 FAIL (T8-11)
#   - 인기 랭킹(rank:drop:popular) 멤버와 dedupe 집합(rank:dedupe:{dropId}) 이 모두 없음.
#     마감 후에는 인기 랭킹 멤버가 조회 시 lazy 제거되므로, dedupe 집합(만료: closeAt + 7d)으로 대신 대조한다
set -euo pipefail
trap 'echo "FAIL  조회 실패 (line $LINENO: $BASH_COMMAND)"; exit 1' ERR

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() { sed -n '6,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2; }
is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }
is_int()  { [[ "${1:-}" =~ ^-?[0-9]+$ ]]; }

[[ $# -ge 1 ]] || usage
DROP_ID=$1; shift
is_uint "$DROP_ID" || { echo "[verify-drop] dropId 는 숫자여야 합니다: $DROP_ID" >&2; usage; }
AFTER_DRAW=false; WAIT_SEC=60; EXPECT=""; KEY_TTL_SEC=86400
while [[ $# -gt 0 ]]; do
  case $1 in
    --after-draw)  AFTER_DRAW=true ;;
    --wait-sec)    [[ $# -ge 2 ]] && is_uint "$2" || usage; WAIT_SEC=$2; shift ;;
    --expect)      [[ $# -ge 2 ]] && is_uint "$2" || usage; EXPECT=$2; shift ;;
    --key-ttl-sec) [[ $# -ge 2 ]] && is_uint "$2" || usage; KEY_TTL_SEC=$2; shift ;;
    -h|--help)     usage ;;
    *) echo "[verify-drop] 알 수 없는 인자: $1" >&2; usage ;;
  esac
  shift
done

sql()   { docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U queuedrop -d entry_db -At -c "$1"; }
# exec -T 는 TTY 가 없어 redis-cli 가 raw 출력을 쓴다 (nil → 빈 줄)
redis() { docker compose exec -T redis redis-cli "$@" | tr -d '\r'; }
FAIL=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1${2:+  ($2)}"; FAIL=1; }
skip() { echo "SKIP  $1  ($2)"; }
# 정수 비교: 두 값이 모두 정수 문자열일 때만 비교하고, 아니면 FAIL (빈 값·소수를 0 으로 보거나 잘라내지 않는다)
cmp() {   # cmp <이름> <a> <op: -eq|-le> <b> [상세]
  if ! is_int "$2" || ! is_int "$4"; then fail "$1" "정수 아님: '$2' $3 '$4'"; return 0; fi
  local ok=false
  case $3 in
    -eq) if [[ "$2" -eq "$4" ]]; then ok=true; fi ;;
    -le) if [[ "$2" -le "$4" ]]; then ok=true; fi ;;
  esac
  if $ok; then pass "$1"; else fail "$1" "${5:-$2 $3 $4}"; fi
}

# 조회 가능 여부 — 인프라가 내려가 있으면 판정하지 않고 실패로 끝낸다
if ! sql "SELECT 1" >/dev/null 2>&1; then echo "FAIL  entry_db 조회 불가 (docker compose postgres 확인)"; exit 1; fi
if [[ "$(redis PING 2>/dev/null || true)" != "PONG" ]]; then echo "FAIL  Redis 조회 불가 (docker compose redis 확인)"; exit 1; fi

SNAP=$(sql "SELECT entry_capacity, winner_count, status FROM drop_snapshots WHERE drop_id=$DROP_ID")
if [[ -z "$SNAP" ]]; then
  echo "FAIL  drop_snapshots 에 dropId=$DROP_ID 없음 (잘못된 dropId 이거나 drop.created 미소비 — scripts/kafka-lag.sh entry-service 확인)"
  exit 1
fi
IFS='|' read -r CAPACITY WINNERS STATUS <<<"$SNAP"

db_count() { sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID"; }
is_closed() { [[ "$(sql "SELECT close_at <= now() FROM drop_snapshots WHERE drop_id=$DROP_ID")" == "t" ]]; }
draw_count() { sql "SELECT COUNT(*) FROM draw_results WHERE drop_id=$DROP_ID"; }

# 응모 키를 한 번에(원자적으로) 읽는다: meta 존재, 잔여, 응모자 수, 저장 확정 수. 잔여 키가 없으면 '-'
ENTRY_LUA="return redis.call('EXISTS', KEYS[1]) .. ' ' .. (redis.call('GET', KEYS[2]) or '-') .. ' ' .. redis.call('SCARD', KEYS[3]) .. ' ' .. redis.call('SCARD', KEYS[4])"
read_entry_keys() {
  read -r META_EXISTS REMAINING APPLICANTS PERSISTED <<<"$(redis EVAL "$ENTRY_LUA" 4 \
    "entry:$DROP_ID:meta" "entry:$DROP_ID:remaining" "entry:$DROP_ID:applicants" "entry:$DROP_ID:persisted")"
}
# 랭킹: 인기 점수(없으면 '-'), dedupe 집합 존재·크기
RANK_LUA="return (redis.call('ZSCORE', KEYS[1], ARGV[1]) or '-') .. ' ' .. redis.call('EXISTS', KEYS[2]) .. ' ' .. redis.call('SCARD', KEYS[2])"
read_rank_keys() {
  read -r POPULAR DEDUPE_EXISTS DEDUPE <<<"$(redis EVAL "$RANK_LUA" 2 rank:drop:popular "rank:dedupe:$DROP_ID" "$DROP_ID")"
}

# --after-draw: 추첨 완료까지 대기 (추첨은 Redis·DB 건수가 맞아야 진행되므로 건수 대기보다 먼저 해도 된다)
RESULTS=$(draw_count)
if $AFTER_DRAW; then
  for ((s = 0; s <= WAIT_SEC; s++)); do
    STATUS=$(sql "SELECT status FROM drop_snapshots WHERE drop_id=$DROP_ID")
    RESULTS=$(draw_count)
    [[ "$STATUS" == "DRAWN" && "$RESULTS" -ge 1 ]] && break
    if ((s < WAIT_SEC)); then sleep 1; fi
  done
  echo "draw wait: status=$STATUS drawResults=$RESULTS (${s}s)"
fi

# 컨슈머 지연 해소 대기: DB 응모 수가 Redis 응모자 수에 도달할 때까지 (meta 가 없으면 대기 없이 한 번 읽는다)
read_entry_keys
DB_COUNT=$(db_count)
if [[ "$META_EXISTS" == "1" ]]; then
  for ((s = 0; s < WAIT_SEC; s++)); do
    [[ "$DB_COUNT" == "$APPLICANTS" && "$DB_COUNT" == "$PERSISTED" ]] && break
    sleep 1
    read_entry_keys
    DB_COUNT=$(db_count)
  done
fi

# 랭킹 반영 대기: 판정할 값(인기 점수 → 없으면 dedupe 크기)이 DB 응모 수와 같아질 때까지.
# 둘 다 없으면 마감 전·응모 있음일 때만 계속 기다린다 (ranking-service 소비 지연)
rank_settled() {
  if [[ "$POPULAR" != "-" ]]; then [[ "$POPULAR" == "$DB_COUNT" ]]
  elif [[ "$DEDUPE_EXISTS" == "1" ]]; then [[ "$DEDUPE" == "$DB_COUNT" ]]
  elif is_closed; then true
  else [[ "$DB_COUNT" == "0" ]]
  fi
}
read_rank_keys
for ((s = 0; s < WAIT_SEC; s++)); do
  if rank_settled; then break; fi
  sleep 1
  read_rank_keys
done

# DUP_USER·DUP_SEQ 는 DB UNIQUE 제약(drop_id,user_id / drop_id,entry_seq) 때문에 항상 0 이다.
# 제약이 빠지는 회귀를 잡기 위해 유지하되, 의미 있는 검사는 아래 건수 비교(Redis ↔ DB ↔ 정원)다.
DUP_USER=$(sql "SELECT COUNT(*) FROM (SELECT user_id FROM entries WHERE drop_id=$DROP_ID GROUP BY user_id HAVING COUNT(*)>1) t")
DUP_SEQ=$(sql "SELECT COUNT(*) FROM (SELECT entry_seq FROM entries WHERE drop_id=$DROP_ID GROUP BY entry_seq HAVING COUNT(*)>1) t")

echo "dropId=$DROP_ID status=$STATUS capacity=$CAPACITY winners=$WINNERS redisKeys=$META_EXISTS remaining=$REMAINING redisApplicants=$APPLICANTS persisted=$PERSISTED db=$DB_COUNT rankingScore=$POPULAR rankingDedupe=$([[ $DEDUPE_EXISTS == 1 ]] && echo "$DEDUPE" || echo -)${EXPECT:+ expect=$EXPECT}"
cmp "NFR-01 DB 응모 수 ≤ 정원" "$DB_COUNT" -le "$CAPACITY"
cmp "NFR-02 사용자 중복 0"     "$DUP_USER" -eq 0
cmp "NFR-02 응모 번호 중복 0"  "$DUP_SEQ" -eq 0

if [[ "$META_EXISTS" == "1" ]]; then
  if [[ "$REMAINING" == "-" ]]; then
    fail "NFR-03 Redis 잔여 키 존재" "entry:$DROP_ID:remaining 없음"
  else
    cmp "NFR-01 Redis 잔여 ≥ 0"                 0 -le "$REMAINING"
    if is_int "$REMAINING" && is_int "$CAPACITY"; then
      cmp "NFR-03 정원 - 잔여 == Redis 응모자 수" $((CAPACITY - REMAINING)) -eq "$APPLICANTS" "$CAPACITY-$REMAINING != $APPLICANTS"
    else
      fail "NFR-03 정원 - 잔여 == Redis 응모자 수" "정수 아님: capacity='$CAPACITY' remaining='$REMAINING'"
    fi
  fi
  cmp "NFR-01 Redis 응모자 수 ≤ 정원"        "$APPLICANTS" -le "$CAPACITY"
  cmp "NFR-03 Redis 응모자 수 == DB 응모 수" "$APPLICANTS" -eq "$DB_COUNT" "redis=$APPLICANTS db=$DB_COUNT (${WAIT_SEC}s 대기 후)"
  cmp "NFR-03 저장 확정(persisted) 수 == DB 응모 수" "$PERSISTED" -eq "$DB_COUNT" "persisted=$PERSISTED db=$DB_COUNT"
else
  # 키가 없어도 되는 경우는 "추첨 기록 시각 + TTL 경과" 뿐이다. 추첨 직후 Redis 재시작으로 유실된 경우(T8-11)는 FAIL
  EXPIRED=$(sql "SELECT drawn_at + make_interval(secs => $KEY_TTL_SEC) <= now() FROM draw_results WHERE drop_id=$DROP_ID")
  if [[ "$STATUS" == "DRAWN" && "$EXPIRED" == "t" ]]; then
    skip "NFR-01·03 Redis 검사" "entry:$DROP_ID:* 없음 — 추첨(drawn_at) 후 ${KEY_TTL_SEC}s 경과, TTL 만료"
  else
    fail "NFR-03 Redis 응모 키 존재" "entry:$DROP_ID:meta 없음, status=$STATUS, drawn_at+TTL 경과=${EXPIRED:-추첨 기록 없음} — Redis 데이터 유실 또는 초기화 누락"
  fi
fi

# 랭킹 점수는 entry.created 소비 결과라, 발행 실패(보상) 경로가 있으면 DB 와 어긋날 수 있다 (FAIL 시 보상 메트릭과 대조)
# 점수 = (dropId, userId) 처음 본 응모 수 (rank.lua dedupe) = dedupe 집합 크기
if [[ "$POPULAR" != "-" ]]; then
  if [[ "$POPULAR" =~ ^[0-9]+$ ]]; then
    cmp "REQ-RANK-03 인기 랭킹 점수 == DB 응모 수" "$POPULAR" -eq "$DB_COUNT" "score=$POPULAR db=$DB_COUNT"
  else
    fail "REQ-RANK-03 인기 랭킹 점수 == DB 응모 수" "점수가 정수 문자열이 아님: '$POPULAR'"
  fi
fi
if [[ "$DEDUPE_EXISTS" == "1" ]]; then
  cmp "REQ-RANK-03 랭킹 집계 사용자 수(rank:dedupe) == DB 응모 수" "$DEDUPE" -eq "$DB_COUNT" "dedupe=$DEDUPE db=$DB_COUNT"
fi
if [[ "$POPULAR" == "-" && "$DEDUPE_EXISTS" != "1" ]]; then
  if is_closed; then                   # 판정 직전에 다시 읽는다
    skip "REQ-RANK-03 인기 랭킹 점수" "마감된 드로우 — 인기 랭킹 멤버(lazy 제거)·rank:dedupe:$DROP_ID 모두 없음"
  elif [[ "$DB_COUNT" == "0" ]]; then
    pass "REQ-RANK-03 인기 랭킹 점수 == DB 응모 수 (응모 0, 멤버 없음)"
  else
    fail "REQ-RANK-03 인기 랭킹 점수 == DB 응모 수" "진행 중인데 rank:drop:popular 멤버·rank:dedupe:$DROP_ID 없음, db=$DB_COUNT (${WAIT_SEC}s 대기 후)"
  fi
fi

if [[ -n "$EXPECT" ]]; then
  cmp "k6 entry_ok == DB 응모 수" "$DB_COUNT" -eq "$EXPECT" "expect=$EXPECT db=$DB_COUNT"
fi

if $AFTER_DRAW; then
  WON=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID AND status='WON'")
  LOST=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID AND status='LOST'")
  APPLIED=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID AND status='APPLIED'")
  DRAW_ROW=$(sql "SELECT applicant_count, winner_count FROM draw_results WHERE drop_id=$DROP_ID")
  IFS='|' read -r R_APPLICANTS R_WINNERS <<<"${DRAW_ROW:-|}"
  EXPECTED=$((DB_COUNT < WINNERS ? DB_COUNT : WINNERS))
  echo "draw: won=$WON lost=$LOST applied=$APPLIED drawResults=$RESULTS recordApplicants=${R_APPLICANTS:--} recordWinners=${R_WINNERS:--}"
  if [[ "$STATUS" == "DRAWN" ]]; then pass "추첨 상태 DRAWN"; else fail "추첨 상태 DRAWN" "status=$STATUS (${WAIT_SEC}s 대기 후)"; fi
  cmp "draw_results 1건"               "$RESULTS" -eq 1
  cmp "당첨 수 == min(K, 응모자 수)"   "$WON" -eq "$EXPECTED" "won=$WON expected=$EXPECTED"
  cmp "미처리(APPLIED) 0"              "$APPLIED" -eq 0
  cmp "당첨 + 낙첨 == DB 응모 수"      $((WON + LOST)) -eq "$DB_COUNT" "$WON+$LOST != $DB_COUNT"
  if [[ -n "$R_APPLICANTS" ]]; then
    cmp "draw_results 응모자 수 == DB 응모 수" "$R_APPLICANTS" -eq "$DB_COUNT" "record=$R_APPLICANTS db=$DB_COUNT"
    cmp "draw_results 당첨 수 == WON 수"       "$R_WINNERS" -eq "$WON" "record=$R_WINNERS won=$WON"
  fi
fi

if ((FAIL)); then echo "결과: FAIL"; else echo "결과: PASS"; fi
exit $FAIL
