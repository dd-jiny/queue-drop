#!/usr/bin/env bash
# E2E 시나리오 테스트 실행 (phase-5 3.6, 방식 C)
#   1) docker compose 로 인프라 기동 (+ 토픽 생성 완료 대기)
#   2) 각 서비스를 e2e 프로필로 백그라운드 기동 (로그: build/e2e/logs/{svc}.log)
#   3) actuator health 대기
#   4) ./gradlew :e2e-test:e2eTest
#   5) 종료 시(성공·실패·중단 모두) trap 으로 서비스 프로세스 정리. 인프라 컨테이너는 남겨 둔다
#
# 사용: scripts/e2e.sh [추가 gradle 인자...]   예) scripts/e2e.sh -Pe2e.adminPassword=...
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="$ROOT_DIR/build/e2e/logs"
HEALTH_TIMEOUT_SEC="${E2E_HEALTH_TIMEOUT_SEC:-180}"

# 서비스:포트 (gateway 는 마지막에 확인). coupon·ranking 은 이 시나리오에 필요 없어 띄우지 않는다
SERVICES=(user-service:8081 product-service:8082 queue-service:8083 entry-service:8085 gateway:8080)

PIDS=()

CLEANED_UP=0
cleanup() {
  local rc=$?
  ((CLEANED_UP == 1)) && return
  CLEANED_UP=1
  if ((${#PIDS[@]} > 0)); then
    echo "[e2e] 서비스 종료: ${PIDS[*]}"
    kill "${PIDS[@]}" 2>/dev/null || true
    # 정상 종료(graceful shutdown) 기다린 뒤 남은 프로세스 강제 종료
    for _ in $(seq 1 30); do
      local alive=0
      for pid in "${PIDS[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
      ((alive == 0)) && break
      sleep 1
    done
    kill -9 "${PIDS[@]}" 2>/dev/null || true
  fi
  echo "[e2e] 서비스 로그: $LOG_DIR"
  exit "$rc"
}
# 정리는 EXIT 에서 한 번만. INT/TERM 은 종료 코드만 정하고 EXIT 트랩으로 넘긴다 (중복 실행 방지)
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 사전 도구 확인
for cmd in docker curl lsof java; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[e2e] 필요한 명령이 없습니다: $cmd" >&2
    exit 1
  fi
done
# 서비스 jar 는 PATH 의 java 로 띄운다. 빌드 toolchain(21)과 맞아야 한다
java_major="$(java -version 2>&1 | head -n 1 | sed -E 's/.*version "([0-9]+).*/\1/')"
if [[ "$java_major" != "21" ]]; then
  echo "[e2e] Java 21 이 필요합니다 (PATH 의 java: $(java -version 2>&1 | head -n 1)). JAVA_HOME/PATH 를 21 로 맞추세요." >&2
  exit 1
fi

port_in_use() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

# 이미 떠 있는 프로세스(e2e 프로필이 아닐 수 있음)를 대상으로 테스트하지 않도록 포트 선점 여부를 먼저 본다
for entry in "${SERVICES[@]}"; do
  port="${entry##*:}"
  if port_in_use "$port"; then
    echo "[e2e] 포트 $port 가 이미 사용 중입니다 (${entry%%:*}). 기존 프로세스를 종료한 뒤 다시 실행하세요." >&2
    exit 1
  fi
done

echo "[e2e] 인프라 기동 (docker compose)"
docker compose up -d
docker compose up -d --wait postgres redis kafka
init_rc="$(docker wait queuedrop-kafka-init)"
if [[ "$init_rc" != "0" ]]; then
  echo "[e2e] 토픽 생성(kafka-init) 실패: exit $init_rc" >&2
  docker logs queuedrop-kafka-init >&2 || true
  exit 1
fi

echo "[e2e] 실행 jar 빌드"
# Gradle 을 서비스마다 병렬로 bootRun 하면 같은 빌드 디렉터리의 락·common 컴파일이 겹친다.
# 한 번에 bootJar 를 만들고 java -jar 로 띄운다 (bootRun 과 같은 실행 결과)
jar_tasks=()
for entry in "${SERVICES[@]}"; do jar_tasks+=(":${entry%%:*}:bootJar"); done
./gradlew "${jar_tasks[@]}" -q

mkdir -p "$LOG_DIR"
for entry in "${SERVICES[@]}"; do
  svc="${entry%%:*}"
  jar="$(ls "$ROOT_DIR/$svc/build/libs/"*.jar | grep -v -- '-plain\.jar$' | head -n 1)"
  echo "[e2e] 기동: $svc ($jar)"
  java -jar "$jar" --spring.profiles.active=e2e >"$LOG_DIR/$svc.log" 2>&1 &
  PIDS+=("$!")
done

echo "[e2e] health 대기 (최대 ${HEALTH_TIMEOUT_SEC}s)"
deadline=$((SECONDS + HEALTH_TIMEOUT_SEC))
for i in "${!SERVICES[@]}"; do
  entry="${SERVICES[$i]}"
  svc="${entry%%:*}"
  port="${entry##*:}"
  pid="${PIDS[$i]}"
  until curl -fsS "http://localhost:$port/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[e2e] $svc 가 기동 중 종료되었습니다. 로그 마지막 부분:" >&2
      tail -n 50 "$LOG_DIR/$svc.log" >&2 || true
      exit 1
    fi
    if ((SECONDS >= deadline)); then
      echo "[e2e] $svc health 대기 시간 초과. 로그 마지막 부분:" >&2
      tail -n 50 "$LOG_DIR/$svc.log" >&2 || true
      exit 1
    fi
    sleep 1
  done
  echo "[e2e] UP: $svc"
done

echo "[e2e] E2E 테스트 실행"
./gradlew :e2e-test:e2eTest "$@"
