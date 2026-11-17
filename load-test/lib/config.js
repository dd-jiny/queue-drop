// 공통 설정 (phase-8 3.4). 값은 전부 -e KEY=VALUE 로 덮어쓴다.
//   compose k6 : BASE_URL=http://gateway:8080, RESULTS_DIR=/results (docker-compose.yml 의 k6 서비스)
//   로컬 k6    : -e BASE_URL=http://localhost:8080 -e RESULTS_DIR=docs/report/results (저장소 루트에서 실행)
export const BASE_URL = __ENV.BASE_URL || 'http://gateway:8080';
export const RESULTS_DIR = __ENV.RESULTS_DIR || '/results';
export const USER_PASSWORD = __ENV.USER_PASSWORD || 'LoadTest!234';
export const ADMIN_EMAIL = __ENV.ADMIN_EMAIL || 'admin@queuedrop.test';
export const ADMIN_PASSWORD = __ENV.ADMIN_PASSWORD || 'Admin!2345';

export const userEmail = (i) => `loadtest-${i}@queuedrop.test`;

// 전역 JSON 객체를 가리지 않도록 HEADERS 로 이름 붙인다
export const HEADERS = { 'Content-Type': 'application/json' };

export function auth(token, tags) {
  return { headers: { ...HEADERS, Authorization: `Bearer ${token}` }, tags };
}

export const int = (name, def) => parseInt(__ENV[name] || `${def}`, 10);
