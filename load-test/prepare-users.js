// 테스트 사용자 사전 생성 (phase-8 3.4, T8-04). 재실행해도 된다: 이미 있으면 409 USER_DUPLICATE_EMAIL
// 실행: docker compose --profile loadtest run --rm k6 run -e TOTAL_USERS=5200 /scripts/prepare-users.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE_URL, HEADERS, USER_PASSWORD, userEmail, int } from './lib/config.js';
import { errorCode } from './lib/api.js';

const TOTAL = int('TOTAL_USERS', 5200);   // S1~S3 최대 USERS(5,000) 와 겹치지 않는 S4 범위(DUP_USER_OFFSET 5,000 + DUP_USERS 200)

http.setResponseCallback(http.expectedStatuses(201, 409));   // 409 = 이미 존재 (재실행 허용)

const created = new Counter('signup_created');
const existed = new Counter('signup_existed');
const unexpected = new Counter('unexpected');                // 201 도 409 USER_DUPLICATE_EMAIL 도 아닌 응답

export const options = {
  scenarios: {
    prepare: { executor: 'shared-iterations', vus: int('PREPARE_VUS', 50), iterations: TOTAL, maxDuration: '20m' },
  },
  thresholds: {
    unexpected: ['count==0'],
    http_req_failed: ['rate==0'],
  },
};

export default function () {
  const i = exec.scenario.iterationInTest + 1;          // 1..TOTAL (VU 와 무관하게 시나리오 안에서 고유)
  const res = http.post(`${BASE_URL}/api/users/signup`,
    JSON.stringify({ email: userEmail(i), password: USER_PASSWORD, nickname: `lt-${i}` }),
    { headers: HEADERS, tags: { name: 'signup', phase: 'prepare' } });
  check(res, { 'created or exists': (r) => r.status === 201 || r.status === 409 });
  if (res.status === 201) created.add(1);
  else if (res.status === 409 && errorCode(res) === 'USER_DUPLICATE_EMAIL') existed.add(1);
  else unexpected.add(1, { status: `${res.status}`, code: errorCode(res) });
}
