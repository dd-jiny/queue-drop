#!/usr/bin/env bash
# 컨슈머 그룹 lag 조회 (phase-8 3.5, 4장 T8-12)
# 사용: scripts/kafka-lag.sh [group ...]   (기본: 컨슈머가 있는 서비스 전부)
#   그룹 이름 = 각 서비스 group-id (${spring.application.name})
#   lag 해소 시간 측정: watch -n 1 scripts/kafka-lag.sh entry-service  → "LAG 합계" 가 0 이 되는 시각 기록
# 종료 코드: 0 조회 성공 / 1 조회 실패(Kafka 미기동·그룹 없음)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# GROUPS 는 bash 예약 변수라 사용 금지
TARGETS=${*:-"product-service queue-service entry-service coupon-service ranking-service"}
FAIL=0
TOTAL=0
for g in $TARGETS; do
  echo "== $g"
  if ! out=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:29092 --describe --group "$g" 2>&1); then
    echo "$out"
    echo "FAIL  $g 조회 실패"
    FAIL=1
    continue
  fi
  if ! grep -q '^GROUP' <<<"$out"; then
    grep -m1 '^Error' <<<"$out" || true      # 스택 트레이스는 생략
    echo "FAIL  $g 오프셋 정보 없음 (그룹 이름 확인)"
    FAIL=1
    continue
  fi
  echo "$out"
  # LAG 열(헤더 기준 위치)이 숫자인 행만 합산. '-' 는 커밋 오프셋이 없는 파티션
  lag=$(awk '/^GROUP/ { for (i = 1; i <= NF; i++) if ($i == "LAG") c = i; next }
             c && $c ~ /^[0-9]+$/ { s += $c } END { print s + 0 }' <<<"$out")
  # LAG '-' = 커밋 오프셋이 없는 파티션 (아직 한 번도 소비하지 않음). 합계에서 빠지므로 따로 센다
  unknown=$(awk '/^GROUP/ { for (i = 1; i <= NF; i++) if ($i == "LAG") c = i; next }
                 c && $c == "-" { n++ } END { print n + 0 }' <<<"$out")
  echo "LAG 합계 $g = $lag (LAG 미확인 '-' 파티션 $unknown)"
  if ((unknown > 0)); then
    echo "WARN  $g: 커밋 오프셋 없는 파티션 ${unknown}개 — 합계 0 이어도 미소비 메시지가 있을 수 있음 (LOG-END-OFFSET 확인)"
  fi
  TOTAL=$((TOTAL + lag))
done
echo "== LAG 합계 전체 = $TOTAL"
exit $FAIL
