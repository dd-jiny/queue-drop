#!/usr/bin/env bash
# 시연 영상 압축 (제출용). 영상 파일(들)을 dist/queue-drop-시연영상.zip 으로 묶는다
# 사용: scripts/package-demo-video.sh <영상 파일> [영상 파일 ...]
#   예) scripts/package-demo-video.sh ~/Movies/queue-drop-demo.mov
# 영상은 이미 압축된 형식(mp4·mov)이라 zip 은 저장(-0)만 해서 시간을 줄인다. 결과 zip 은 커밋하지 않는다(dist/ 는 .gitignore)
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if (($# == 0)); then
  echo "사용: $0 <영상 파일> [영상 파일 ...]" >&2; exit 2
fi
for f in "$@"; do
  [[ -f "$f" ]] || { echo "[package] 파일 없음: $f" >&2; exit 2; }
done
OUT_DIR="$ROOT_DIR/dist"; OUT="$OUT_DIR/queue-drop-시연영상.zip"
mkdir -p "$OUT_DIR"; rm -f "$OUT"
zip -j -0 "$OUT" "$@" >/dev/null
echo "[package] 생성: $OUT ($(du -h "$OUT" | cut -f1))"
unzip -l "$OUT" | tail -n +2
