// 로그인 · 관리자 API · 대기 · 결과 요약 (phase-8 3.4)
import http from 'k6/http';
import { check, sleep } from 'k6';
// k6 v2.3·v2.4 에서 동작 확인 (2026-11-17). 네트워크가 막힌 환경이면 이 파일을 lib/ 로 내려받아 상대 경로로 바꾼다
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';
import { BASE_URL, HEADERS, RESULTS_DIR, auth } from './config.js';

export function login(email, password, tags = { name: 'login', phase: 'prepare' }) {
  const res = http.post(`${BASE_URL}/api/users/login`,
    JSON.stringify({ email, password }), { headers: HEADERS, tags });
  check(res, { 'login 200': (r) => r.status === 200 });
  return res.status === 200 ? res.json('accessToken') : null;
}

/** 관리자 API 응답이 기대와 다르면 setup 을 즉시 중단한다 (id 없이 부하를 걸면 결과가 전부 404 가 된다) */
function adminPost(url, body, token, expected, label) {
  const res = http.post(url, JSON.stringify(body), auth(token, { name: 'admin', phase: 'setup' }));
  check(res, { [`${label} ${expected}`]: (r) => r.status === expected });
  if (res.status !== expected) {
    throw new Error(`${label} 실패: status=${res.status} body=${res.body}`);
  }
  return res.json('id');
}

export function adminLogin(email, password) {
  const token = login(email, password, { name: 'admin', phase: 'setup' });
  if (!token) throw new Error(`관리자 로그인 실패: ${email} (user-service ADMIN_EMAIL/ADMIN_PASSWORD 확인)`);
  return token;
}

export function createProduct(adminToken, name) {
  return adminPost(`${BASE_URL}/api/products`,
    { name, description: 'load test', imageUrl: null }, adminToken, 201, 'product');
}

export function createDrop(adminToken, productId, d) {
  return adminPost(`${BASE_URL}/api/products/${productId}/drops`, d, adminToken, 201, 'drop');
}

export function createCouponEvent(adminToken, e) {
  return adminPost(`${BASE_URL}/api/coupons/events`, e, adminToken, 201, 'coupon event');
}

/** 목표 시각(epoch ms)까지 대기. jitterMs 만큼 무작위로 늦게 출발 (사람마다 클릭 시점이 다름) */
export function waitUntil(epochMs, jitterMs = 0) {
  const ms = epochMs - Date.now() + Math.random() * jitterMs;
  if (ms > 0) sleep(ms / 1000);
}

export const errorCode = (res) => {
  try { return res.json('code') || 'UNKNOWN'; } catch (e) { return 'UNKNOWN'; }
};

/**
 * handleSummary 공통: 콘솔 요약 + RESULTS_DIR/{name}-{epochMs}.json (data.setup_data 는 k6 v2 에서도 노출됨).
 * 출력을 파일로 넘길 때는 -e NO_COLOR=1 로 ANSI 색을 끈다.
 */
export function summary(data, name) {
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: !__ENV.NO_COLOR }),
    [`${RESULTS_DIR}/${name}-${Date.now()}.json`]: JSON.stringify(data, null, 2),
  };
}
