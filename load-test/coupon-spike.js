// 쿠폰 스파이크 — 시나리오 S3(spike)·S4(dup_burst, 같은 사용자 동시 BURST 회) (phase-8 3.4, T8-08·T8-09)
//   S3·S4 는 쿠폰 이벤트를 따로 만든다: S3 는 수량 소진, S4 는 수량 여유(DUP_QUANTITY ≥ DUP_USERS)로 "중복 차단" 만으로 거절되게 한다
// 실행: docker compose --profile loadtest run --rm k6 run -e USERS=3000 -e QUANTITY=500 /scripts/coupon-spike.js
// 확대: docker compose --profile loadtest run --rm k6 run -e USERS=5000 -e QUANTITY=1000 -e START_DELAY_SEC=120 -e LOGIN_SPREAD_SEC=90 /scripts/coupon-spike.js
// 사전 조건: prepare-users.js 로 loadtest-1..(DUP_USER_OFFSET + DUP_USERS) 생성 (기본 5,200)
// 사용자별 202 개수는 k6 안에서 모을 수 없다 → S4 202 합계 ≤ DUP_USERS 임계값 + verify-coupon.sh <dupEventId> 로 판정
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD, USER_PASSWORD, userEmail, auth, int } from './lib/config.js';
import { login, adminLogin, createCouponEvent, waitUntil, errorCode, summary } from './lib/api.js';

const USERS = int('USERS', 3000);
const QUANTITY = int('QUANTITY', 500);              // S3 이벤트 수량
const DUP_USERS = int('DUP_USERS', 200);            // S4 참여 사용자 수
const BURST = int('BURST', 5);                      // 한 사용자의 동시 요청 수
const DUP_QUANTITY = int('DUP_QUANTITY', 1000);     // S4 이벤트 수량 (DUP_USERS 보다 넉넉하게)
const DUP_USER_OFFSET = int('DUP_USER_OFFSET', 5000);   // S4 사용자 번호 시작 (spike 사용자와 겹치지 않게)
const START_DELAY_SEC = int('START_DELAY_SEC', 60);     // 이벤트 등록 → 발급 시작. 모든 VU 의 로그인이 이 안에 끝나야 한다
const JITTER_MS = int('JITTER_MS', 500);
const LOGIN_SPREAD_SEC = int('LOGIN_SPREAD_SEC', 0);    // 로그인을 0~N초에 균등 분산 (0 = 모든 VU 즉시 로그인). spike·dup_burst 공통

if (DUP_USER_OFFSET < USERS) throw new Error('DUP_USER_OFFSET 은 USERS 이상이어야 한다');
if (DUP_QUANTITY < DUP_USERS) throw new Error('DUP_QUANTITY 는 DUP_USERS 이상이어야 한다');
if (START_DELAY_SEC < 5) throw new Error('START_DELAY_SEC 는 5 이상이어야 한다 (issueStartAt 은 미래여야 하고 로그인 시간이 필요)');
if (LOGIN_SPREAD_SEC < 0 || LOGIN_SPREAD_SEC >= START_DELAY_SEC - 10) {
  throw new Error('LOGIN_SPREAD_SEC 는 0 이상, START_DELAY_SEC - 10 미만이어야 한다 (분산된 로그인이 발급 시작 전에 끝날 여유)');
}

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409));

const couponOk = new Counter('coupon_ok');                 // scenario 태그로 S3·S4 구분 (k6 기본 태그)
const couponRejected = new Counter('coupon_rejected');     // tag: code
const couponRetryable = new Counter('coupon_retryable_503');
const couponNotStarted = new Counter('coupon_not_started');   // COUPON_NOT_STARTED (1회 재시도)
const unexpected = new Counter('unexpected');
const loginLate = new Counter('login_late');               // 로그인이 발급 시작 이후에 끝남 → START_DELAY_SEC 부족

export const options = {
  setupTimeout: '2m',
  scenarios: {
    spike: { executor: 'per-vu-iterations', vus: USERS, iterations: 1, exec: 'spike', maxDuration: '10m' },
    dup_burst: { executor: 'per-vu-iterations', vus: DUP_USERS * BURST, iterations: 1, exec: 'dupBurst', maxDuration: '10m' },
  },
  thresholds: {
    'http_req_duration{name:coupon_issue}': ['p(95)<200', 'p(99)<500'],
    'http_req_failed{phase:open}': ['rate<0.001'],
    // 수량 초과 없음 (정확히 QUANTITY 인지는 verify-coupon.sh 로 DB·Redis 와 함께 본다)
    'coupon_ok{scenario:spike}': [`count<=${QUANTITY}`],
    // 수량이 넉넉하므로 202 는 사용자당 정확히 1개 → 넘으면 중복 발급, 모자라면 발급 유실
    'coupon_ok{scenario:dup_burst}': [`count==${DUP_USERS}`],
    unexpected: ['count==0'],
    checks: ['rate==1'],
    login_late: ['count==0'],
    // 정합성 위반 신호: spike 사용자는 1회만 요청 → DUPLICATE 불가, S4 는 수량 여유 → SOLD_OUT 불가
    'coupon_rejected{scenario:spike,code:COUPON_DUPLICATE}': ['count==0'],
    'coupon_rejected{scenario:dup_burst,code:COUPON_SOLD_OUT}': ['count==0'],
    // 관찰용 (항상 통과): 태그별 서브메트릭을 요약·결과 JSON 에 남기기 위한 선언 (3.6 관찰 지표)
    'coupon_rejected{scenario:spike,code:COUPON_SOLD_OUT}': ['count>=0'],
    'coupon_rejected{scenario:dup_burst,code:COUPON_DUPLICATE}': ['count>=0'],
    'http_req_duration{name:coupon_issue_burst}': ['p(99)>=0'],
  },
};

export function setup() {
  const admin = adminLogin(ADMIN_EMAIL, ADMIN_PASSWORD);
  const startAtMs = Date.now() + START_DELAY_SEC * 1000;
  const issueStartAt = new Date(startAtMs).toISOString();
  const eventId = createCouponEvent(admin,
    { name: `coupon-spike-${QUANTITY}`, totalQuantity: QUANTITY, issueStartAt });
  const dupEventId = createCouponEvent(admin,
    { name: `coupon-dup-${DUP_USERS}x${BURST}`, totalQuantity: DUP_QUANTITY, issueStartAt });
  console.log(`couponEventId=${eventId} dupCouponEventId=${dupEventId} issueStartAt=${issueStartAt}`);
  return { eventId, dupEventId, startAtMs };
}

/** 발급 요청 1건. COUPON_NOT_STARTED(시계 차이로 이르게 도착)면 1회만 재시도 */
function issue(token, eventId, name) {
  const url = `${BASE_URL}/api/coupons/events/${eventId}/issue`;
  let res = http.post(url, null, auth(token, { name, phase: 'open' }));
  if (res.status === 409 && errorCode(res) === 'COUPON_NOT_STARTED') {
    couponNotStarted.add(1);
    sleep(1);
    res = http.post(url, null, auth(token, { name: `${name}_retry`, phase: 'open' }));
  }
  return res;
}

function record(res) {
  if (res.status === 202) { couponOk.add(1); return; }
  if (res.status === 409 && errorCode(res) !== 'COUPON_NOT_STARTED') {   // COUPON_SOLD_OUT, COUPON_DUPLICATE
    couponRejected.add(1, { code: errorCode(res) }); return;
  }
  if (res.status === 503) { couponRetryable.add(1); return; }
  unexpected.add(1, { status: `${res.status}`, code: errorCode(res) });   // 재시도 후에도 NOT_STARTED 포함
}

export function spike(data) {
  const i = exec.scenario.iterationInTest + 1;                          // 1..USERS
  if (LOGIN_SPREAD_SEC > 0) sleep(Math.random() * LOGIN_SPREAD_SEC);    // 로그인 분산 (측정 대상은 발급 시작 이후 구간)
  const token = login(userEmail(i), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }
  if (Date.now() > data.startAtMs) loginLate.add(1);
  waitUntil(data.startAtMs, JITTER_MS);
  record(issue(token, data.eventId, 'coupon_issue'));
}

export function dupBurst(data) {
  // BURST 개의 연속 반복이 같은 사용자 → 사용자당 동시 BURST 회
  const i = DUP_USER_OFFSET + Math.floor(exec.scenario.iterationInTest / BURST) + 1;
  // 로그인 분산은 burst 동시성에 영향 없음: 같은 사용자의 BURST 개 VU 가 각자 다른 시각에 로그인해도
  // 발급 요청은 아래 waitUntil(startAtMs, 0) 으로 같은 시각에 모인다
  if (LOGIN_SPREAD_SEC > 0) sleep(Math.random() * LOGIN_SPREAD_SEC);
  const token = login(userEmail(i), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }
  if (Date.now() > data.startAtMs) loginLate.add(1);
  waitUntil(data.startAtMs, 0);                     // 같은 사용자의 요청이 겹치도록 jitter 없음
  record(issue(token, data.dupEventId, 'coupon_issue_burst'));
}

export function handleSummary(data) {
  const s = data.setup_data || {};
  return summary(data, `coupon-${s.eventId || 'unknown'}-${s.dupEventId || 'unknown'}`);
}
