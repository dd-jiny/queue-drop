#!/usr/bin/env bash
# 부하 테스트 환경 기동 + 컨테이너 점검 (phase-8 3.3 실행 순서, 4장 T8-01~T8-03)
#   1) 인프라 기동 + 토픽 생성(kafka-init) 완료 확인
#   2) app 프로필(서비스 7개) 기동, healthy 대기          → T8-01
#   3) 호스트에서 8081~8086 연결 불가, 8080 만 응답       → T8-02
#   4) X-User-Id 헤더만 붙여 gateway 로 응모 → 401        → T8-03
#
# 사용: JWT_SECRET=... scripts/loadtest-up.sh [--build] [--check-only]
#   --build      이미지 재빌드 (사전에 ./gradlew clean bootJar 필요)
#   --check-only 기동 없이 T8-01~03 점검만
# 종료 코드: 점검 하나라도 실패하면 1. 컨테이너는 내리지 않는다 (내리기: docker compose --profile app stop gateway user-service product-service queue-service coupon-service entry-service ranking-service — 서비스를 지정하지 않으면 인프라까지 멈춘다)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BUILD_FLAG=""
CHECK_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --build) BUILD_FLAG="--build" ;;
    --check-only) CHECK_ONLY=1 ;;
    *) echo "[loadtest-up] 알 수 없는 인자: $arg" >&2; exit 2 ;;
  esac
done

APP_SERVICES=(gateway user-service product-service queue-service coupon-service entry-service ranking-service)
INTERNAL_PORTS=(8081 8082 8083 8084 8085 8086)
FAILED=0
pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAILED=1; }

if ((CHECK_ONLY == 0)); then
  if [[ -z "${JWT_SECRET:-}" ]]; then
    echo "[loadtest-up] JWT_SECRET 이 비어 있습니다. 예) export JWT_SECRET=\$(openssl rand -base64 48)" >&2
    exit 1
  fi
  echo "[loadtest-up] 인프라 기동"
  # 인자 없는 up 으로 kafka-init 까지 만든다 (postgres redis kafka 만 지정하면 새 환경에서 kafka-init 컨테이너가 없어 docker wait 가 실패)
  docker compose up -d
  docker compose up -d --wait postgres redis kafka
  init_rc="$(docker wait queuedrop-kafka-init)"
  if [[ "$init_rc" != "0" ]]; then
    echo "[loadtest-up] 토픽 생성(kafka-init) 실패: exit $init_rc" >&2
    docker logs queuedrop-kafka-init >&2 || true
    exit 1
  fi

  echo "[loadtest-up] 서비스 기동 (app 프로필)"
  # shellcheck disable=SC2086
  if ! docker compose --profile app up -d $BUILD_FLAG --wait; then
    echo "[loadtest-up] 서비스 기동 실패" >&2
  fi
fi

echo "[loadtest-up] T8-01 서비스 7개 healthy"
for svc in "${APP_SERVICES[@]}"; do
  cid="$(docker compose --profile app ps -q "$svc" 2>/dev/null || true)"
  if [[ -z "$cid" ]]; then fail "$svc: 컨테이너 없음"; continue; fi
  health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid")"
  if [[ "$health" == "healthy" ]]; then pass "$svc healthy"; else
    fail "$svc: $health (최근 로그)"
    docker logs --tail 20 "$cid" 2>&1 | sed 's/^/      /'
  fi
done

echo "[loadtest-up] T8-02 내부 포트 비노출"
for port in "${INTERNAL_PORTS[@]}"; do
  if curl -s -o /dev/null --max-time 3 "http://localhost:$port/actuator/health"; then
    fail "localhost:$port 이 응답함 (노출되면 안 됨)"
  else
    pass "localhost:$port 연결 불가"
  fi
done
if curl -fs --max-time 5 http://localhost:8080/actuator/health >/dev/null; then
  pass "localhost:8080/actuator/health 응답"
else
  fail "localhost:8080/actuator/health 응답 없음"
fi

echo "[loadtest-up] T8-03 헤더 위조 차단"
status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
  -H 'X-User-Id: 1' -H 'Content-Type: application/json' \
  http://localhost:8080/api/entries/drops/1)"
if [[ "$status" == "401" ]]; then pass "X-User-Id 만 붙인 응모 → 401"; else fail "X-User-Id 만 붙인 응모 → $status (기대 401)"; fi

if ((FAILED == 1)); then
  echo "[loadtest-up] 점검 실패"
  exit 1
fi
echo "[loadtest-up] 점검 통과"
