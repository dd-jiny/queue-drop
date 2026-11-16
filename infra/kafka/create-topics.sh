#!/bin/bash
# 토픽 사전 생성. 여러 번 실행해도 안전하다(--if-not-exists).
set -euo pipefail

BOOTSTRAP="kafka:29092"
TOPICS_BIN="/opt/kafka/bin/kafka-topics.sh"

create() {
  local topic=$1 partitions=$2; shift 2
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
  echo "created: $topic (partitions=$partitions $*)"
}

# 저빈도 이벤트: 파티션 1
create user.registered 1
create drop.created    1
create draw.completed  1

# 피크 이벤트: 컨슈머 병렬 처리를 위해 파티션 6
# entry.created 는 랭킹 재집계를 위해 30일 보존 (30 * 24 * 60 * 60 * 1000 ms)
create entry.created  6 --config retention.ms=2592000000
create coupon.issued  6

# DLT: 저장 컨슈머(entry, coupon)가 3회 재시도 후 보내는 토픽.
# DeadLetterPublishingRecoverer 기본 동작은 원본과 같은 파티션 번호로 보내므로 파티션 수를 원본과 맞춘다.
create drop.created.DLT  1
create entry.created.DLT 6
create coupon.issued.DLT 6

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list
