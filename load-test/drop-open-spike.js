// 드로우 오픈 스파이크 — 시나리오 S1·S2 (phase-8 3.4, T8-05·T8-07)
//   사용자 1명 = VU 1개: 로그인(prepare) → 오픈 시각 대기 → 대기열 진입 → 폴링 → 응모
// 실행 예:
//   S1: docker compose --profile loadtest run --rm k6 run -e USERS=3000 -e CAPACITY=1000 -e WINNERS=100 /scripts/drop-open-spike.js
//   S2: docker compose --profile loadtest run --rm k6 run -e USERS=5000 -e CAPACITY=100 -e WINNERS=10 /scripts/drop-open-spike.js
//   확대: docker compose --profile loadtest run --rm k6 run -e USERS=10000 -e CAPACITY=3000 -e WINNERS=300 -e OPEN_DELAY_SEC=120 -e LOGIN_SPREAD_SEC=90 /scripts/drop-open-spike.js
// 사전 조건: prepare-users.js 로 loadtest-1..USERS 생성
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD, USER_PASSWORD, userEmail, auth, int } from './lib/config.js';
import { login, adminLogin, createProduct, createDrop, waitUntil, errorCode, summary } from './lib/api.js';

const USERS = int('USERS', 3000);
const CAPACITY = int('CAPACITY', 1000);          // 응모 정원 M
const WINNERS = int('WINNERS', 100);             // 당첨 수량 K
const OPEN_DELAY_SEC = int('OPEN_DELAY_SEC', 90);    // 드로우 등록 → 오픈. 서버 규칙: openAt ≥ 등록 시각 + 1분 (Drop.MIN_OPEN_LEAD_TIME)
const ENTRY_WINDOW_SEC = int('ENTRY_WINDOW_SEC', 300);
const JITTER_MS = int('JITTER_MS', 1000);
const POLL_MS = int('POLL_MS', 1000);
const MAX_WAIT_SEC = int('MAX_WAIT_SEC', 600);
const ENTER_RETRY = int('ENTER_RETRY', 10);       // 첫 진입 재시도 상한 (1초 간격)
const LOGIN_SPREAD_SEC = int('LOGIN_SPREAD_SEC', 0);  // 로그인을 0~N초에 균등 분산 (0 = 모든 VU 즉시 로그인). 실제 로그인은 오픈 전 시간에 퍼져 있다

if (OPEN_DELAY_SEC < 61) throw new Error('OPEN_DELAY_SEC 는 61 이상이어야 한다 (드로우 오픈 1분 여유 규칙)');
if (LOGIN_SPREAD_SEC < 0 || LOGIN_SPREAD_SEC >= OPEN_DELAY_SEC - 10) {
  throw new Error('LOGIN_SPREAD_SEC 는 0 이상, OPEN_DELAY_SEC - 10 미만이어야 한다 (분산된 로그인이 오픈 전에 끝날 여유)');
}
if (WINNERS > CAPACITY) throw new Error('WINNERS 는 CAPACITY 이하여야 한다 (M ≥ K)');

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409));

const queueWait = new Trend('queue_wait_ms', true);         // 진입 → 입장까지 걸린 시간
const entryOk = new Counter('entry_ok');
const entryRejected = new Counter('entry_rejected');        // tag: code
const entryRetryable = new Counter('entry_retryable_503');
const queueSoldOut = new Counter('queue_sold_out');         // tag: code (진입 409 QUEUE_SOLD_OUT / 폴링 중 SOLD_OUT 은 POLL_SOLD_OUT)
const queueClosed = new Counter('queue_closed');            // 진입 409 QUEUE_CLOSED (응모 기간 종료)
const queueLost = new Counter('queue_lost');                // 토큰 만료 등으로 NOT_IN_QUEUE
const queueTimeout = new Counter('queue_timeout');
const queueEnterRetry = new Counter('queue_enter_retry');   // tag: code (QUEUE_NOT_OPEN / QUEUE_DROP_NOT_FOUND)
const unexpected = new Counter('unexpected');               // tag: step, status, code
const loginLate = new Counter('login_late');                // 로그인이 오픈 시각 이후에 끝남 → OPEN_DELAY_SEC 부족 (측정 구간 오염)

export const options = {
  setupTimeout: '2m',
  scenarios: {
    drop_open: {
      executor: 'per-vu-iterations',
      vus: USERS,
      iterations: 1,
      maxDuration: `${OPEN_DELAY_SEC + MAX_WAIT_SEC + 120}s`,
    },
  },
  thresholds: {
    'http_req_duration{name:queue_enter}': ['p(95)<300', 'p(99)<800'],
    'http_req_duration{name:entry_apply}': ['p(95)<300', 'p(99)<500'],
    'http_req_failed{phase:open}': ['rate<0.001'],
    unexpected: ['count==0'],
    checks: ['rate==1'],
    login_late: ['count==0'],
    // 정합성: 202 합계가 정원을 넘으면 초과 판정 (정확한 값은 verify-drop.sh 로 DB·Redis 와 함께 본다)
    entry_ok: [`count<=${CAPACITY}`],
    // 정합성: 사용자마다 응모 1회뿐이라 ENTRY_DUPLICATE 가 나오면 판정 오류
    'entry_rejected{code:ENTRY_DUPLICATE}': ['count==0'],
    // 관찰용 (항상 통과): 태그별 서브메트릭을 요약·결과 JSON 에 남기기 위한 선언 (3.6 관찰 지표)
    'entry_rejected{code:ENTRY_SOLD_OUT}': ['count>=0'],
    'entry_rejected{code:ENTRY_NO_TOKEN}': ['count>=0'],
    'entry_rejected{code:ENTRY_CLOSED}': ['count>=0'],
    'queue_sold_out{code:QUEUE_SOLD_OUT}': ['count>=0'],
    'queue_sold_out{code:POLL_SOLD_OUT}': ['count>=0'],
    queue_closed: ['count>=0'],
    'queue_enter_retry{code:QUEUE_NOT_OPEN}': ['count>=0'],
    'queue_enter_retry{code:QUEUE_DROP_NOT_FOUND}': ['count>=0'],
  },
};

export function setup() {
  const admin = adminLogin(ADMIN_EMAIL, ADMIN_PASSWORD);
  const productId = createProduct(admin, `load-test-${Date.now()}`);
  const openAtMs = Date.now() + OPEN_DELAY_SEC * 1000;
  const dropId = createDrop(admin, productId, {
    title: `spike-${USERS}u-${CAPACITY}c`,
    entryCapacity: CAPACITY,
    winnerCount: WINNERS,
    openAt: new Date(openAtMs).toISOString(),
    closeAt: new Date(openAtMs + ENTRY_WINDOW_SEC * 1000).toISOString(),
  });
  console.log(`dropId=${dropId} openAt=${new Date(openAtMs).toISOString()}`);
  return { dropId, openAtMs };
}

export default function (data) {
  const i = exec.scenario.iterationInTest + 1;              // 1..USERS (per-vu-iterations·iterations 1 → VU 마다 고유)
  if (LOGIN_SPREAD_SEC > 0) sleep(Math.random() * LOGIN_SPREAD_SEC);   // 로그인 분산 (측정 대상은 오픈 이후 구간)
  const token = login(userEmail(i), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }
  if (Date.now() > data.openAtMs) loginLate.add(1);

  waitUntil(data.openAtMs, JITTER_MS);

  // 1) 대기열 진입
  const tags = (name) => auth(token, { name, phase: 'open' });
  // 재시도 대상 (DropFlowE2ETest 와 같은 기준):
  //   409 QUEUE_NOT_OPEN       — 시계 차이로 살짝 이르게 도착
  //   404 QUEUE_DROP_NOT_FOUND — queue-service 가 drop.created 를 아직 소비하지 않음
  const retryable = (r) => (r.status === 409 && errorCode(r) === 'QUEUE_NOT_OPEN')
                        || (r.status === 404 && errorCode(r) === 'QUEUE_DROP_NOT_FOUND');
  let res = http.post(`${BASE_URL}/api/queue/drops/${data.dropId}`, null, tags('queue_enter'));
  for (let attempt = 0; attempt < ENTER_RETRY && retryable(res); attempt++) {
    queueEnterRetry.add(1, { code: errorCode(res) });
    sleep(1);
    res = http.post(`${BASE_URL}/api/queue/drops/${data.dropId}`, null, tags('queue_enter_retry'));
  }
  if (res.status === 409 && errorCode(res) === 'QUEUE_SOLD_OUT') { queueSoldOut.add(1, { code: 'QUEUE_SOLD_OUT' }); return; }
  if (res.status === 409 && errorCode(res) === 'QUEUE_CLOSED') { queueClosed.add(1); return; }
  if (res.status !== 200) {                                 // 재시도 소진(QUEUE_NOT_OPEN·404)·그 밖의 409 포함
    unexpected.add(1, { step: 'enter', status: `${res.status}`, code: errorCode(res) }); return;
  }

  // 2) 폴링 (nextPollAfterMs 는 WAITING 일 때만 양수, 그 외 0)
  let state = res.json('state');
  const enteredAt = Date.now();
  while (state === 'WAITING' && Date.now() - enteredAt < MAX_WAIT_SEC * 1000) {
    sleep((res.json('nextPollAfterMs') || POLL_MS) / 1000);
    res = http.get(`${BASE_URL}/api/queue/drops/${data.dropId}`, tags('queue_status'));
    if (res.status !== 200) { unexpected.add(1, { step: 'status', status: `${res.status}`, code: errorCode(res) }); return; }
    state = res.json('state');
  }

  if (state === 'SOLD_OUT') { queueSoldOut.add(1, { code: 'POLL_SOLD_OUT' }); return; }
  if (state === 'NOT_IN_QUEUE') { queueLost.add(1); return; }
  if (state !== 'ADMITTED') { queueTimeout.add(1); return; }
  queueWait.add(Date.now() - enteredAt);

  // 3) 응모 (503 이면 사용자처럼 1회 재시도)
  for (let attempt = 0; attempt < 2; attempt++) {
    const apply = http.post(`${BASE_URL}/api/entries/drops/${data.dropId}`, null,
      tags(attempt === 0 ? 'entry_apply' : 'entry_apply_retry'));
    if (apply.status === 202) {
      check(apply, { 'entrySeq 존재': (r) => r.json('entrySeq') > 0 });
      entryOk.add(1);
      return;
    }
    if (apply.status === 409) { entryRejected.add(1, { code: errorCode(apply) }); return; }
    if (apply.status === 503) { entryRetryable.add(1); sleep(0.2); continue; }
    unexpected.add(1, { step: 'apply', status: `${apply.status}`, code: errorCode(apply) });
    return;
  }
}

export function handleSummary(data) {
  const dropId = data.setup_data ? data.setup_data.dropId : 'unknown';
  return summary(data, `drop-open-${dropId}`);
}
