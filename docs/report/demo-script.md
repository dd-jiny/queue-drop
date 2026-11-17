# 시연 영상 대본

녹화 길이 목표 6~8분. 화면 배치: 왼쪽 브라우저(시연 페이지 http://localhost:8000), 오른쪽 터미널. 3장에서 Grafana(http://localhost:3000) 탭을 연다.

## 0. 녹화 전 준비 (영상에 넣지 않음)
```bash
export JWT_SECRET=$(openssl rand -base64 48)
docker compose up -d                                   # 인프라 (postgres, redis, kafka, kafka-ui)
./gradlew clean bootJar -q                             # 이미지가 없을 때만
QUEUE_ADMISSION_BATCH_SIZE=3 scripts/loadtest-up.sh --build   # 서비스 7개 + 점검 T8-01~03. 배치 3 = 순번이 줄어드는 게 보이게
docker compose --profile demo up -d demo               # 시연 페이지
docker compose --profile monitoring up -d              # Grafana (3장)
```
- 테스트 사용자(loadtest-1..20000)가 없으면: `k6 run -e BASE_URL=http://localhost:8080 -e TOTAL_USERS=5200 load-test/prepare-users.js`
- 브라우저 확대 125% 정도, 터미널 글자 크게. 녹화는 macOS `Cmd + Shift + 5` → "전체 화면 기록".

## 1. 소개 (30초)
- 시연 페이지 첫 화면에서: "대기열 → 선착순 응모(정원 M) → 마감 시각 자동 추첨(K명), 선착순 쿠폰, 랭킹. 서비스 7개, gateway 하나만 외부 노출."
- 터미널: `docker compose ps` — 서비스 7개 healthy.

## 2. 드로우 흐름 — 대기열·응모·추첨 (2분 30초)
1. **관리자 로그인** → 관리자 탭 → 상품 + 드로우 등록 (오픈 65초 후, 응모 기간 1분, 정원 M=30, 당첨 K=5).
2. **가상 사용자 투입** 40명 (같은 드로우) — 오픈 시각에 한꺼번에 대기열 진입.
3. 로그아웃 → **일반 사용자 로그인**(테스트 사용자 번호, 예: 7004) → 드로우 탭 → 방금 만든 드로우 선택 → "오픈되면 자동 진입" 체크.
4. 오픈 → **대기 순번이 40 → 0 으로 줄어드는 화면**. 오른쪽 API 로그에 `GET /api/queue/...` 의 `aheadCount` 가 줄어드는 것.
   - 설명: "입장은 1초마다 배치 수만큼. 응모 API 에 닿는 부하를 대기열이 제한한다."
5. 입장되면 **응모하기** → 202 (정원 안) 또는 `ENTRY_SOLD_OUT`(매진). 다시 누르면 `ENTRY_DUPLICATE`.
   - 정원 30 · 41명이라 일부는 매진 — "정원보다 많이 받지 않는다" 를 보여주는 장면.
6. 마감 후 **추첨 결과** (당첨/낙첨) — 마감 후 5초 안에 자동 추첨.
7. 터미널: `scripts/verify-drop.sh <dropId> --after-draw` → 전부 PASS (정원 초과 0, 중복 0, Redis = DB, 추첨 결과 일치).

## 3. 부하 — 3,000명 동시 오픈 (2분)
1. Grafana 대시보드 "queue-drop 부하 테스트" 를 띄워 둔다.
2. 터미널 (배치 100 으로 되돌린 뒤 실행하면 대기 시간이 짧다 — 녹화 시간을 줄이려면 `QUEUE_ADMISSION_BATCH_SIZE=100 docker compose --profile app up -d --wait queue-service && docker restart queuedrop-gateway`):
   ```bash
   k6 run -e BASE_URL=http://localhost:8080 -e RESULTS_DIR=docs/report/results \
     -e USERS=3000 -e CAPACITY=1000 -e WINNERS=100 load-test/drop-open-spike.js
   ```
3. 오픈 순간 Grafana: 처리량 급증, p99, 입장 속도, consumer lag 0 유지. (로그인 90초 대기 구간은 빨리 감기)
4. 끝나면 k6 요약의 `entry_ok 1000`, `unexpected 0` → `scripts/verify-drop.sh <dropId> --expect 1000` → **PASS**.
   - 설명: "3,000명이 동시에 몰려도 정원 1,000 을 정확히 지킨다."

## 4. 쿠폰 — 선착순 + 중복 방지 (1분 30초)
1. 시연 페이지 관리자 탭 → 쿠폰 이벤트 등록(수량 2, 시작 20초 후) → 쿠폰 탭에서 발급 → 202, 다시 누르면 "이미 발급"(`COUPON_DUPLICATE`), 다른 사용자로 발급 → 남은 수량 0 이면 매진.
2. 터미널 — 쿠폰 스파이크 + 같은 사용자 동시 5회:
   ```bash
   k6 run -e BASE_URL=http://localhost:8080 -e RESULTS_DIR=docs/report/results \
     -e USERS=3000 -e QUANTITY=500 load-test/coupon-spike.js
   ```
   → `coupon_ok{spike} 500`, `coupon_ok{dup_burst} 200`, `COUPON_DUPLICATE 800` → `scripts/verify-coupon.sh <eventId> --expect 500`, `scripts/verify-coupon.sh <dupEventId> --expect 200` → **PASS**.
   - 설명: "200명이 각각 5번 동시에 눌러도 1장씩만."

## 5. 내구성 — Redis 재시작 (40초)
```bash
docker compose restart redis
scripts/verify-drop.sh <3장 dropId> --expect 1000      # 재시작 후에도 PASS (AOF)
```

## 6. 랭킹 · 마무리 (30초)
- 시연 페이지 랭킹 탭: 인기 드로우(응모 수), 사용자 랭킹(주간/전체/내 순위).
- 마무리 멘트: "모든 부하 실행에서 정원·수량 초과 0, 중복 0. 측정으로 찾은 병목 4개를 고쳤고, 상세는 결과보고서 7장."

## 녹화 후
```bash
scripts/package-demo-video.sh ~/Movies/<녹화 파일>.mov    # → dist/queue-drop-시연영상.zip
docker compose --profile app --profile monitoring --profile demo stop   # 정리
```
