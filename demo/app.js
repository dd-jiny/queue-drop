/*
 * queue-drop 시연 페이지 (바닐라 JS, 외부 의존 없음)
 * - 모든 요청은 같은 출처의 /api 로 보내고 nginx 가 gateway 로 프록시한다
 * - 토큰은 sessionStorage 에 둔다 (접근 실패해도 동작하도록 try/catch)
 */
'use strict';

const ADMIN = { email: 'admin@queuedrop.test', password: 'Admin!2345' };
const TEST_PASSWORD = 'LoadTest!234';
const testEmail = (n) => `loadtest-${n}@queuedrop.test`;
const TOKEN_TTL_MS = 300 * 1000;   // 입장 토큰 TTL (QUEUE_ADMISSION_TOKEN_TTL_SECONDS 기본값). 응답에 없어 화면에서는 추정치로 표시
const QUEUE_POLL_MS = 1000;
const RESULT_POLL_MS = 3000;
const RESULT_POLL_LIMIT_MS = 5 * 60 * 1000;

// 에러 code → 화면 문구. 없으면 서버 message 를 그대로 쓴다
const MSG = {
  AUTH_TOKEN_MISSING: '로그인이 필요합니다.',
  AUTH_INVALID_TOKEN: '유효하지 않은 토큰입니다. 다시 로그인하세요.',
  AUTH_EXPIRED_TOKEN: '토큰이 만료되었습니다. 다시 로그인하세요.',
  COMMON_FORBIDDEN: '관리자 권한이 필요합니다.',
  USER_INVALID_CREDENTIALS: '이메일 또는 비밀번호가 올바르지 않습니다.',
  USER_DUPLICATE_EMAIL: '이미 가입된 이메일입니다.',
  PRODUCT_INVALID_DROP: '드로우 정보가 올바르지 않습니다. (오픈은 지금 + 1분 이후, 당첨 K ≤ 정원 M)',
  PRODUCT_DROP_NOT_FOUND: '드로우를 찾을 수 없습니다.',
  QUEUE_NOT_OPEN: '아직 오픈 전입니다.',
  QUEUE_CLOSED: '응모가 마감되었습니다.',
  QUEUE_SOLD_OUT: '응모 정원이 모두 찼습니다. (매진)',
  QUEUE_DROP_NOT_FOUND: '대기열 정보가 없는 드로우입니다. (등록 직후라면 잠시 후 다시 시도)',
  ENTRY_NO_TOKEN: '입장 토큰이 없거나 만료되었습니다. 대기열에 다시 진입하세요.',
  ENTRY_DUPLICATE: '이미 응모한 드로우입니다.',
  ENTRY_SOLD_OUT: '응모 정원이 마감되었습니다. (매진)',
  ENTRY_CLOSED: '응모 기간이 아닙니다.',
  ENTRY_NOT_FOUND: '응모 내역이 없습니다.',
  ENTRY_TEMPORARILY_FAILED: '일시적인 오류입니다. 다시 시도해 주세요.',
  ENTRY_DRAW_NOT_COMPLETED: '아직 추첨이 완료되지 않았습니다.',
  COUPON_EVENT_NOT_FOUND: '쿠폰 이벤트를 찾을 수 없습니다.',
  COUPON_NOT_STARTED: '아직 발급 시작 전입니다.',
  COUPON_DUPLICATE: '이미 발급받은 쿠폰입니다.',
  COUPON_SOLD_OUT: '쿠폰이 모두 소진되었습니다. (매진)',
  COUPON_TEMPORARILY_FAILED: '일시적인 오류입니다. 다시 시도해 주세요.',
};

const STATUS_KO = { SCHEDULED: '오픈 예정', OPEN: '응모 중', CLOSED: '마감', DRAWN: '추첨 완료' };
const ENTRY_STATUS_KO = { APPLIED: '응모 완료', WON: '당첨', LOST: '낙첨' };

// ---------- 유틸 ----------
const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const store = {
  get(k) { try { return sessionStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { sessionStorage.setItem(k, v); } catch { /* 저장 불가 환경은 무시 */ } },
  del(k) { try { sessionStorage.removeItem(k); } catch { /* 무시 */ } },
};

const pad2 = (n) => String(n).padStart(2, '0');
const hms = (d) => `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
function fmtTime(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  const t = hms(d);
  const sameDay = d.toDateString() === new Date().toDateString();
  return sameDay ? t : `${d.getMonth() + 1}/${d.getDate()} ${t}`;
}
function fmtDur(ms) {
  const s = Math.max(0, Math.ceil(ms / 1000));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  const pad = (n) => String(n).padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(sec)}` : `${pad(m)}:${pad(sec)}`;
}
function errText(r) {
  let msg = MSG[r.code] || r.message || `HTTP ${r.status}`;
  if (r.data && Array.isArray(r.data.fieldErrors) && r.data.fieldErrors.length) {
    msg += ' — ' + r.data.fieldErrors.map((f) => `${f.field}: ${f.reason}`).join(', ');
  }
  return msg;
}

let toastTimer;
function toast(text, kind = '') {
  const el = $('toast');
  el.textContent = text;
  el.className = `toast ${kind}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add('hidden'), 2800);
}

function setBox(prefix, kind, label, big, sub) {
  const box = $(`${prefix}-box`);
  const bigEl = box.querySelector('.status-big');
  const changed = bigEl.textContent !== String(big);
  box.className = `status-box ${kind}`;
  box.querySelector('.status-label').textContent = label;
  bigEl.textContent = big;
  box.querySelector('.status-sub').textContent = sub;
  if (changed) { void box.offsetWidth; box.classList.add('pulse'); }
}

// ---------- 상태 ----------
const S = {
  token: store.get('qd.token'),
  email: store.get('qd.email'),
  claims: null,
  drops: [],
  drop: null,            // 선택한 드로우
  queueTimer: null,
  queueState: null,
  admittedAt: null,
  applied: false,
  resultTimer: null,
  resultPollStartedAt: null,
  drawDone: false,
  autoEnterFired: false,
  coupon: null,
  couponTimer: null,
  rankingTimer: null,
  bots: null,
};

// ---------- API ----------
async function api(method, path, { body, auth = true, log = true, token } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const bearer = token || (auth ? S.token : null);
  if (bearer) headers.Authorization = `Bearer ${bearer}`;

  const t0 = performance.now();
  let status = 0, data = null, text = '';
  try {
    const res = await fetch(path, { method, headers, body: body !== undefined ? JSON.stringify(body) : undefined });
    status = res.status;
    text = await res.text();
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
  } catch (e) {
    text = `네트워크 오류: ${e.message}`;
  }
  const ms = Math.round(performance.now() - t0);
  if (log) addLog(method, path, status, ms, data !== null ? JSON.stringify(data) : (text || '(본문 없음)'));

  const r = { ok: status >= 200 && status < 300, status, data, ms, code: data && data.code, message: (data && data.message) || text };
  // 내 토큰이 만료·무효면 로그아웃 처리 (가상 사용자 토큰은 제외)
  if (!token && bearer && status === 401 && (r.code === 'AUTH_EXPIRED_TOKEN' || r.code === 'AUTH_INVALID_TOKEN')) {
    logout();
    toast(errText(r), 'err');
  }
  return r;
}

// ---------- API 로그 ----------
let lastLog = null;
function addLog(method, path, status, ms, body) {
  const list = $('log-list');
  const time = hms(new Date());
  const statusClass = `s${String(status)[0] || 0}`;
  // 연속으로 같은 요청(폴링 등)은 한 줄로 묶어 횟수만 늘린다
  if (lastLog && lastLog.key === `${method} ${path} ${status}` && lastLog.el.isConnected) {
    lastLog.count += 1;
    lastLog.el.querySelector('.log-time').textContent = time;
    lastLog.el.querySelector('.log-ms').textContent = `${ms}ms`;
    lastLog.el.querySelector('.log-count').textContent = `×${lastLog.count}`;
    lastLog.el.querySelector('.log-body').textContent = body;
    lastLog.el.classList.remove('fresh'); void lastLog.el.offsetWidth; lastLog.el.classList.add('fresh');
    return;
  }
  const li = document.createElement('li');
  li.className = 'fresh';
  li.innerHTML = `<div class="log-line"><span class="log-time">${time}</span><span class="log-method">${esc(method)}</span>`
    + `<span class="log-path">${esc(path)}</span><span class="log-status ${statusClass}">${status || 'ERR'}</span>`
    + `<span class="log-ms">${ms}ms</span><span class="log-count"></span></div><div class="log-body"></div>`;
  li.querySelector('.log-body').textContent = body;
  li.addEventListener('click', () => li.classList.toggle('open'));
  list.prepend(li);
  while (list.children.length > 200) list.lastChild.remove();
  lastLog = { key: `${method} ${path} ${status}`, el: li, count: 1 };
}
function addNote(text) {
  lastLog = null;
  const li = document.createElement('li');
  li.className = 'fresh';
  li.innerHTML = `<div class="log-line"><span class="log-time">${hms(new Date())}</span>`
    + `<span class="log-method">BOT</span><span class="log-path"></span></div>`;
  li.querySelector('.log-path').textContent = text;
  $('log-list').prepend(li);
}

// ---------- 인증 ----------
function decodeJwt(token) {
  try {
    const p = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const json = decodeURIComponent(atob(p).split('').map((c) => '%' + c.charCodeAt(0).toString(16).padStart(2, '0')).join(''));
    return JSON.parse(json);
  } catch { return null; }
}

function setSession(token, email) {
  S.token = token; S.email = email; S.claims = token ? decodeJwt(token) : null;
  if (token) { store.set('qd.token', token); store.set('qd.email', email || ''); }
  else { store.del('qd.token'); store.del('qd.email'); }
  renderSession();
}

function logout() {
  stopQueuePolling();
  stopResultPolling();
  setSession(null, null);
  resetDropBoxes();
}

function renderSession() {
  const logged = !!S.token && !!S.claims;
  $('login-form').classList.toggle('hidden', logged);
  $('session').classList.toggle('hidden', !logged);
  const isAdmin = logged && S.claims.role === 'ADMIN';
  $('admin-warn').classList.toggle('hidden', isAdmin);
  if (logged) {
    $('session-role').textContent = S.claims.role || '-';
    $('session-role').className = `badge ${S.claims.role || ''}`;
    $('session-user').textContent = `${S.email || ''} (#${S.claims.sub})`;
  }
  renderDropActions();
  renderCouponActions();
}

async function login(email, password) {
  const r = await api('POST', '/api/users/login', { body: { email, password }, auth: false });
  if (!r.ok) { toast(errText(r), 'err'); return false; }
  setSession(r.data.accessToken, email);
  toast(`${email} 로그인`, 'ok');
  if (S.drop) restoreDropState();
  return true;
}

// ---------- 드로우 ----------
function dropPhase(d, now = Date.now()) {
  if (!d) return null;
  if (d.status === 'DRAWN') return 'DRAWN';
  if (now < Date.parse(d.openAt)) return 'SCHEDULED';
  if (now < Date.parse(d.closeAt)) return 'OPEN';
  return 'CLOSED';
}

async function loadDrops() {
  const r = await api('GET', '/api/products/drops?size=100', { auth: false });
  const ul = $('drop-list');
  if (!r.ok) { ul.innerHTML = `<li class="muted">${esc(errText(r))}</li>`; return; }
  S.drops = (r.data.content || []).slice().sort((a, b) => b.id - a.id).slice(0, 30);
  if (!S.drops.length) { ul.innerHTML = '<li class="muted">등록된 드로우가 없습니다. 관리자 탭에서 등록하세요.</li>'; return; }
  ul.innerHTML = '';
  for (const d of S.drops) {
    const li = document.createElement('li');
    li.dataset.id = d.id;
    li.className = S.drop && S.drop.id === d.id ? 'selected' : '';
    const ph = dropPhase(d);
    li.innerHTML = `<span class="drop-id">#${d.id}</span><span class="drop-title"></span>`
      + `<span class="drop-meta">M ${d.entryCapacity} · K ${d.winnerCount}</span><span class="badge ${ph}">${STATUS_KO[ph]}</span>`;
    li.querySelector('.drop-title').textContent = d.title;
    li.addEventListener('click', () => selectDrop(d.id));
    ul.appendChild(li);
  }
}

async function selectDrop(id) {
  stopQueuePolling();
  stopResultPolling();
  S.queueState = null; S.admittedAt = null; S.applied = false; S.drawDone = false; S.autoEnterFired = false;
  resetDropBoxes();
  const r = await api('GET', `/api/products/drops/${id}`, { auth: false });
  if (!r.ok) { toast(errText(r), 'err'); return; }
  S.drop = r.data;
  store.set('qd.drop', String(id));
  $('f-bot-drop').value = id;
  document.querySelectorAll('#drop-list li').forEach((li) => li.classList.toggle('selected', Number(li.dataset.id) === id));
  renderDropDetail();
  renderDropActions();
  restoreDropState();
}

function renderDropDetail() {
  const d = S.drop;
  const el = $('drop-detail');
  if (!d) { el.innerHTML = '<div class="empty muted">왼쪽 목록에서 드로우를 선택하세요.</div>'; return; }
  el.innerHTML = `<div class="card-head"><h2>드로우 #${d.id}</h2><span id="detail-badge" class="badge"></span></div>
    <p class="detail-title"></p>
    <div class="detail-grid">
      <div><span>응모 정원 M</span><b>${d.entryCapacity}명</b></div>
      <div><span>당첨 K</span><b>${d.winnerCount}명</b></div>
      <div><span>오픈</span><b>${fmtTime(d.openAt)}</b></div>
      <div><span>마감</span><b>${fmtTime(d.closeAt)}</b></div>
    </div>
    <div id="drop-countdown" class="countdown"></div>`;
  el.querySelector('.detail-title').textContent = d.title;
  tickDrop();
}

function tickDropList(now) {
  // 목록 배지도 시각에 맞춰 갱신 (오픈 예정 → 응모 중 → 마감)
  document.querySelectorAll('#drop-list li[data-id]').forEach((li) => {
    const d = S.drops.find((x) => x.id === Number(li.dataset.id));
    const badge = li.querySelector('.badge');
    if (!d || !badge) return;
    const ph = S.drop && S.drop.id === d.id ? dropPhase(S.drop, now) : dropPhase(d, now);
    if (!badge.classList.contains(ph)) { badge.className = `badge ${ph}`; badge.textContent = STATUS_KO[ph]; }
  });
}

function tickDrop() {
  const now = Date.now();
  tickDropList(now);
  const d = S.drop;
  if (!d) return;
  const ph = dropPhase(d, now);
  const badge = $('detail-badge');
  if (badge) { badge.className = `badge ${ph}`; badge.textContent = STATUS_KO[ph]; }
  const cd = $('drop-countdown');
  if (cd) {
    cd.className = `countdown ${ph}`;
    if (ph === 'SCHEDULED') cd.innerHTML = `오픈까지<b>${fmtDur(Date.parse(d.openAt) - now)}</b>`;
    else if (ph === 'OPEN') cd.innerHTML = `응모 마감까지<b>${fmtDur(Date.parse(d.closeAt) - now)}</b>`;
    else if (ph === 'CLOSED') cd.innerHTML = `응모 마감<b>추첨 대기</b>`;
    else cd.innerHTML = `추첨 완료<b>결과 공개</b>`;
  }
  // 오픈되면 자동 진입
  if (ph === 'OPEN' && $('queue-auto-enter').checked && !S.autoEnterFired && S.token && !S.queueState) {
    S.autoEnterFired = true;
    enterQueue();
  }
  // 입장 토큰 남은 시간 (추정)
  if (S.queueState === 'ADMITTED' && S.admittedAt && !S.applied) {
    const left = TOKEN_TTL_MS - (now - S.admittedAt);
    if (left > 0) $('queue-sub').textContent = `입장 토큰 유효 약 ${fmtDur(left)} (TTL 5분 기준 추정)`;
    else setBox('queue', 'err', '토큰 만료(추정)', '재진입', '입장 토큰이 만료됐을 수 있습니다. 대기열에 다시 진입하세요.');
  }
  // 마감 후 추첨 결과 자동 확인
  if ((ph === 'CLOSED' || ph === 'DRAWN') && S.applied && !S.drawDone && !S.resultTimer && S.token) startResultPolling();
  renderDropActions();
}

function renderDropActions() {
  const logged = !!S.token;
  const hasDrop = !!S.drop;
  const ph = dropPhase(S.drop);
  $('btn-queue-enter').disabled = !(logged && hasDrop) || ph === 'CLOSED' || ph === 'DRAWN';
  $('btn-queue-stop').disabled = !S.queueTimer;
  $('btn-entry-apply').disabled = !(logged && hasDrop && S.queueState === 'ADMITTED' && !S.applied);
  $('btn-entry-me').disabled = !(logged && hasDrop);
  $('btn-draw-result').disabled = !hasDrop;
}

function resetDropBoxes() {
  setBox('queue', 'idle', '대기열 미진입', '-', '드로우를 선택하고 오픈 후 대기열에 진입하세요.');
  setBox('entry', 'idle', '응모 전', '-', '입장되면 응모할 수 있습니다.');
}

// 드로우를 다시 고르거나 새로고침했을 때 서버 상태로 화면을 복원한다
async function restoreDropState() {
  if (!S.drop || !S.token) return;
  const dropId = S.drop.id;
  const me = await api('GET', `/api/entries/drops/${dropId}/me`);
  if (!S.drop || S.drop.id !== dropId) return;
  if (me.ok) {
    S.applied = true;
    renderMyEntry(me.data);
    renderDropActions();
    const ph = dropPhase(S.drop);
    if (ph === 'CLOSED' || ph === 'DRAWN') checkDrawResult(true);   // 추첨 요약(응모·당첨 인원)까지 표시
    return;
  }
  const q = await api('GET', `/api/queue/drops/${dropId}`);
  if (q.ok && q.data.state !== 'NOT_IN_QUEUE') handleQueueStatus(q.data, true);
}

// ---------- 대기열 ----------
async function enterQueue() {
  if (!S.drop) return;
  stopQueuePolling();
  const dropId = S.drop.id;
  const r = await api('POST', `/api/queue/drops/${dropId}`);
  if (!r.ok) {
    setBox('queue', 'err', r.code || `HTTP ${r.status}`, '진입 실패', errText(r));
    return;
  }
  handleQueueStatus(r.data, true);
}

function handleQueueStatus(st, schedule) {
  const prev = S.queueState;
  S.queueState = st.state;
  if (st.state === 'WAITING') {
    const ahead = st.aheadCount ?? 0;
    setBox('queue', 'wait', '대기 중', ahead === 0 ? '곧 입장' : `${ahead.toLocaleString()}명`,
      ahead === 0 ? '내 차례입니다. 다음 입장 주기에 들어갑니다.' : '내 앞의 대기 인원');
    if (schedule) scheduleQueuePoll(Math.max(QUEUE_POLL_MS, Math.min(st.nextPollAfterMs || QUEUE_POLL_MS, 3000)));
  } else if (st.state === 'ADMITTED') {
    if (prev !== 'ADMITTED') { S.admittedAt = Date.now(); toast('입장했습니다! 지금 응모하세요.', 'ok'); }
    setBox('queue', 'ok', '입장 완료', '응모 가능', `입장 토큰 유효 약 ${fmtDur(TOKEN_TTL_MS - (Date.now() - S.admittedAt))} (TTL 5분 기준 추정)`);
    if (!S.applied) setBox('entry', 'info', '응모 가능', '지금!', '“응모하기”를 누르세요. 선착순 정원 안에 들면 응모가 확정됩니다.');
    stopQueuePolling();
  } else if (st.state === 'SOLD_OUT') {
    setBox('queue', 'err', '매진', 'SOLD OUT', '응모 정원이 모두 찼습니다.');
    if (!S.applied) setBox('entry', 'err', '매진', 'SOLD OUT', '대기 중에 응모 정원이 모두 찼습니다.');
    stopQueuePolling();
  } else {
    setBox('queue', 'idle', '대기열에 없음', '-', '아직 진입하지 않았거나 입장 토큰이 만료되었습니다.');
    stopQueuePolling();
  }
  renderDropActions();
}

function scheduleQueuePoll(ms) {
  clearTimeout(S.queueTimer);
  const dropId = S.drop.id;
  S.queueTimer = setTimeout(async () => {
    const r = await api('GET', `/api/queue/drops/${dropId}`);
    if (!S.queueTimer || !S.drop || S.drop.id !== dropId) return;
    if (!r.ok) { setBox('queue', 'err', r.code || `HTTP ${r.status}`, '조회 실패', errText(r)); stopQueuePolling(); return; }
    handleQueueStatus(r.data, true);
  }, ms);
  renderDropActions();
}

function stopQueuePolling() {
  clearTimeout(S.queueTimer);
  S.queueTimer = null;
  if ($('btn-queue-stop')) $('btn-queue-stop').disabled = true;
}

// ---------- 응모 · 추첨 ----------
async function applyEntry() {
  if (!S.drop) return;
  const btn = $('btn-entry-apply');
  btn.disabled = true;
  const r = await api('POST', `/api/entries/drops/${S.drop.id}`);
  if (r.ok) {
    S.applied = true;
    setBox('entry', 'ok', '응모 완료 (202)', `${r.data.entrySeq}번째`, `선착순 ${r.data.entrySeq}번째로 응모했습니다. 정원 ${S.drop.entryCapacity}명 · 마감 후 ${S.drop.winnerCount}명 추첨`);
    setBox('queue', 'ok', '입장 완료', '응모함', '입장 토큰은 응모와 함께 소진되었습니다.');
    toast('응모 완료!', 'ok');
  } else if (r.code === 'ENTRY_DUPLICATE') {
    S.applied = true;
    setBox('entry', 'info', '중복 응모', '이미 응모', errText(r));
  } else if (r.code === 'ENTRY_SOLD_OUT' || r.code === 'QUEUE_SOLD_OUT') {
    setBox('entry', 'err', '매진', 'SOLD OUT', errText(r));
  } else {
    setBox('entry', 'err', r.code || `HTTP ${r.status}`, '응모 실패', errText(r));
  }
  renderDropActions();
}

function renderMyEntry(e, summary) {
  const st = e.status;
  const seq = e.entrySeq != null ? `선착순 ${e.entrySeq}번째` : '저장 대기 중(비동기)';
  if (st === 'WON') {
    setBox('entry', 'ok', '추첨 결과', '당첨', summary ? `응모 ${summary.applicantCount}명 중 ${summary.winnerCount}명 당첨 · ${seq}` : seq);
    S.drawDone = true;
  } else if (st === 'LOST') {
    setBox('entry', 'err', '추첨 결과', '낙첨', summary ? `응모 ${summary.applicantCount}명 중 ${summary.winnerCount}명 당첨 · ${seq}` : seq);
    S.drawDone = true;
  } else {
    setBox('entry', 'ok', '응모 완료', e.entrySeq != null ? `${e.entrySeq}번째` : '응모함', `${seq} · 마감 후 추첨 결과가 공개됩니다.`);
  }
  if (S.queueState !== 'ADMITTED') setBox('queue', 'ok', '응모함', '✓', '이 드로우에 이미 응모했습니다.');
}

async function checkMyEntry() {
  if (!S.drop) return;
  const r = await api('GET', `/api/entries/drops/${S.drop.id}/me`);
  if (r.ok) { S.applied = true; renderMyEntry(r.data); }
  else setBox('entry', r.status === 404 ? 'idle' : 'err', r.code || `HTTP ${r.status}`, '응모 없음', errText(r));
  renderDropActions();
}

async function checkDrawResult(quiet) {
  if (!S.drop) return;
  const dropId = S.drop.id;
  const sum = await api('GET', `/api/entries/drops/${dropId}/result`, { auth: false });
  if (!S.drop || S.drop.id !== dropId) return;
  if (!sum.ok) {
    if (!quiet) toast(errText(sum), 'err');
    if (sum.code === 'ENTRY_DRAW_NOT_COMPLETED' && S.applied) setBox('entry', 'wait', '추첨 대기', '추첨 중…', '마감 후 추첨 스케줄러가 곧 결과를 확정합니다.');
    return;
  }
  if (!S.token) {
    setBox('entry', 'info', '추첨 완료', `${sum.data.winnerCount}명`, `응모 ${sum.data.applicantCount}명 중 ${sum.data.winnerCount}명 당첨 · seed ${sum.data.seed}`);
    return;
  }
  const me = await api('GET', `/api/entries/drops/${dropId}/me`);
  if (me.ok) {
    renderMyEntry(me.data, sum.data);
    if (me.data.status === 'WON' || me.data.status === 'LOST') stopResultPolling();
  } else {
    setBox('entry', 'info', '추첨 완료', `${sum.data.winnerCount}명`, `응모 ${sum.data.applicantCount}명 중 ${sum.data.winnerCount}명 당첨 (나는 응모하지 않음)`);
    stopResultPolling();
  }
  if (S.drop.status !== 'DRAWN') { S.drop.status = 'DRAWN'; tickDrop(); }
}

function startResultPolling() {
  S.resultPollStartedAt = Date.now();
  const loop = async () => {
    await checkDrawResult(true);
    if (!S.resultTimer) return;
    if (S.drawDone || Date.now() - S.resultPollStartedAt > RESULT_POLL_LIMIT_MS) { stopResultPolling(); return; }
    S.resultTimer = setTimeout(loop, RESULT_POLL_MS);
  };
  S.resultTimer = setTimeout(loop, 500);
}
function stopResultPolling() { clearTimeout(S.resultTimer); S.resultTimer = null; }

async function loadMyEntries() {
  const tb = $('my-entries');
  if (!S.token) { tb.innerHTML = '<tr><td colspan="4" class="muted">로그인이 필요합니다.</td></tr>'; return; }
  const r = await api('GET', '/api/entries/me');
  if (!r.ok) { tb.innerHTML = `<tr><td colspan="4" class="muted">${esc(errText(r))}</td></tr>`; return; }
  if (!r.data.length) { tb.innerHTML = '<tr><td colspan="4" class="muted">응모 내역이 없습니다.</td></tr>'; return; }
  tb.innerHTML = r.data.map((e) => `<tr><td>#${e.dropId}</td><td>${e.entrySeq ?? '저장 대기'}</td>`
    + `<td><span class="badge ${e.status === 'WON' ? 'OPEN' : e.status === 'LOST' ? 'CLOSED' : 'DRAWN'}">${ENTRY_STATUS_KO[e.status] || esc(e.status)}</span></td>`
    + `<td>${fmtTime(e.appliedAt)}</td></tr>`).join('');
}

// ---------- 쿠폰 ----------
async function loadCouponEvent(id, log = true) {
  const r = await api('GET', `/api/coupons/events/${id}`, { auth: false, log });
  if (!r.ok) {
    if (log) { $('coupon-info').innerHTML = `<span class="muted">${esc(errText(r))}</span>`; S.coupon = null; renderCouponActions(); }
    return;
  }
  const first = !S.coupon || S.coupon.id !== r.data.id;
  S.coupon = r.data;
  store.set('qd.coupon', String(id));
  const c = r.data;
  $('coupon-info').innerHTML = `<p class="detail-title"></p><div class="detail-grid">
      <div><span>이벤트 ID</span><b>#${c.id}</b></div>
      <div><span>발급 시작</span><b>${fmtTime(c.issueStartAt)}</b></div>
      <div><span>총 수량</span><b>${c.totalQuantity}장</b></div>
      <div><span>남은 수량</span><b>${c.remaining}장</b></div></div>`;
  $('coupon-info').querySelector('.detail-title').textContent = c.name;
  $('coupon-countdown').classList.remove('hidden');
  if (first) setBox('coupon', 'idle', '발급 전', '-', '발급 시작 후 “쿠폰 발급”을 누르세요.');
  tickCoupon();
  renderCouponActions();
}

function tickCoupon() {
  const c = S.coupon;
  const el = $('coupon-countdown');
  if (!c) { el.classList.add('hidden'); return; }
  const left = Date.parse(c.issueStartAt) - Date.now();
  if (left > 0) { el.className = 'countdown SCHEDULED'; el.innerHTML = `발급 시작까지<b>${fmtDur(left)}</b>`; }
  else if (c.remaining <= 0) { el.className = 'countdown CLOSED'; el.innerHTML = '발급 종료<b>소진</b>'; }
  else { el.className = 'countdown OPEN'; el.innerHTML = `발급 중 · 남은 수량<b>${c.remaining}장</b>`; }
}

function renderCouponActions() {
  $('btn-coupon-issue').disabled = !(S.token && S.coupon);
}

async function issueCoupon() {
  if (!S.coupon) return;
  const id = S.coupon.id;
  const r = await api('POST', `/api/coupons/events/${id}/issue`);
  if (r.ok) {
    setBox('coupon', 'ok', '발급 성공 (202)', '발급 완료', `발급 시각 ${fmtTime(r.data.issuedAt)} · 저장은 비동기로 처리됩니다.`);
    toast('쿠폰 발급 성공!', 'ok');
    setTimeout(loadMyCoupons, 1200);
  } else if (r.code === 'COUPON_DUPLICATE') {
    setBox('coupon', 'info', '중복 발급', '이미 발급', errText(r));
  } else if (r.code === 'COUPON_SOLD_OUT') {
    setBox('coupon', 'err', '매진', 'SOLD OUT', errText(r));
  } else if (r.code === 'COUPON_NOT_STARTED') {
    setBox('coupon', 'wait', '발급 전', '아직!', errText(r));
  } else {
    setBox('coupon', 'err', r.code || `HTTP ${r.status}`, '발급 실패', errText(r));
  }
  loadCouponEvent(id);
}

async function loadMyCoupons() {
  const tb = $('my-coupons');
  if (!S.token) { tb.innerHTML = '<tr><td colspan="3" class="muted">로그인이 필요합니다.</td></tr>'; return; }
  const r = await api('GET', '/api/coupons/me');
  if (!r.ok) { tb.innerHTML = `<tr><td colspan="3" class="muted">${esc(errText(r))}</td></tr>`; return; }
  if (!r.data.length) { tb.innerHTML = '<tr><td colspan="3" class="muted">발급받은 쿠폰이 없습니다.</td></tr>'; return; }
  tb.innerHTML = r.data.map((c) => `<tr><td>#${c.couponEventId}</td><td>${esc(c.eventName)}</td><td>${fmtTime(c.issuedAt)}</td></tr>`).join('');
}

// ---------- 랭킹 ----------
async function loadRankings() {
  const myId = S.claims ? Number(S.claims.sub) : null;
  const [drops, weekly, all] = await Promise.all([
    api('GET', '/api/rankings/drops?size=10', { auth: false }),
    api('GET', '/api/rankings/users/weekly?size=10', { auth: false }),
    api('GET', '/api/rankings/users/all?size=10', { auth: false }),
  ]);
  const empty = (n, t) => `<tr><td colspan="${n}" class="muted">${t}</td></tr>`;
  $('rank-drops').innerHTML = !drops.ok ? empty(3, esc(errText(drops))) : !drops.data.length ? empty(3, '아직 응모가 없습니다.')
    : drops.data.map((d) => `<tr class="${S.drop && S.drop.id === d.dropId ? 'me' : ''}"><td>${d.rank}</td><td>#${d.dropId} ${esc(d.title)}</td><td class="num">${d.entryCount.toLocaleString()}</td></tr>`).join('');
  const userRows = (r) => !r.ok ? empty(3, esc(errText(r))) : !r.data.items.length ? empty(3, '아직 점수가 없습니다.')
    : r.data.items.map((u) => `<tr class="${u.userId === myId ? 'me' : ''}"><td>${u.rank}</td><td>${esc(u.nickname || `#${u.userId}`)}</td><td class="num">${u.score.toLocaleString()}</td></tr>`).join('');
  $('rank-weekly').innerHTML = userRows(weekly);
  $('rank-week').textContent = weekly.ok && weekly.data.week ? `(${weekly.data.week})` : '';
  $('rank-all').innerHTML = userRows(all);

  const meEl = $('rank-me');
  if (!S.token) { meEl.className = 'rank-me muted'; meEl.textContent = '로그인하면 표시됩니다.'; return; }
  const me = await api('GET', '/api/rankings/users/me');
  if (!me.ok) { meEl.className = 'rank-me muted'; meEl.textContent = errText(me); return; }
  const m = me.data;
  meEl.className = 'rank-me';
  meEl.innerHTML = `<div class="stat"><span>주간 순위 (${esc(m.week)})</span><b>${m.weeklyRank != null ? `${m.weeklyRank}위` : '-'}</b><span>${m.weeklyScore}점</span></div>`
    + `<div class="stat"><span>전체 순위</span><b>${m.allRank != null ? `${m.allRank}위` : '-'}</b><span>${m.allScore}점</span></div>`;
}

function setRankingAuto(on) {
  clearInterval(S.rankingTimer);
  S.rankingTimer = on ? setInterval(() => { if (currentTab() === 'ranking') loadRankings(); }, 3000) : null;
}

// ---------- 관리자 ----------
async function registerDrop(ev) {
  ev.preventDefault();
  const out = $('drop-form-result');
  const name = $('f-product-name').value.trim();
  const title = $('f-drop-title').value.trim();
  const openAfter = Number($('f-open-after').value);
  const duration = Number($('f-duration').value);
  const capacity = Number($('f-capacity').value);
  const winners = Number($('f-winners').value);

  const p = await api('POST', '/api/products', { body: { name, description: '시연용 상품', imageUrl: null } });
  if (!p.ok) { out.innerHTML = `<p class="warn">상품 등록 실패: ${esc(errText(p))}</p>`; return; }
  const openAt = new Date(Date.now() + openAfter * 1000);
  const closeAt = new Date(openAt.getTime() + duration * 1000);
  const d = await api('POST', `/api/products/${p.data.id}/drops`, {
    body: { title, entryCapacity: capacity, winnerCount: winners, openAt: openAt.toISOString(), closeAt: closeAt.toISOString() },
  });
  if (!d.ok) { out.innerHTML = `<p class="warn">상품 #${p.data.id} 등록, 드로우 등록 실패: ${esc(errText(d))}</p>`; return; }
  out.innerHTML = `<div class="pill-row"><span class="badge DRAWN">상품 #${p.data.id}</span><span class="badge OPEN">드로우 #${d.data.id}</span>`
    + `<span class="muted">오픈 ${fmtTime(d.data.openAt)} · 마감 ${fmtTime(d.data.closeAt)}</span>`
    + `<button type="button" class="btn small primary" id="go-drop">드로우 화면으로</button></div>`;
  $('f-bot-drop').value = d.data.id;
  $('go-drop').addEventListener('click', () => { showTab('drop'); selectDrop(d.data.id); });
  toast(`드로우 #${d.data.id} 등록`, 'ok');
  loadDrops();
}

async function registerCouponEvent(ev) {
  ev.preventDefault();
  const out = $('coupon-form-result');
  const startAt = new Date(Date.now() + Number($('f-coupon-after').value) * 1000);
  const r = await api('POST', '/api/coupons/events', {
    body: { name: $('f-coupon-name').value.trim(), totalQuantity: Number($('f-coupon-qty').value), issueStartAt: startAt.toISOString() },
  });
  if (!r.ok) { out.innerHTML = `<p class="warn">등록 실패: ${esc(errText(r))}</p>`; return; }
  out.innerHTML = `<div class="pill-row"><span class="badge OPEN">이벤트 #${r.data.id}</span>`
    + `<span class="muted">${r.data.totalQuantity}장 · 시작 ${fmtTime(r.data.issueStartAt)}</span>`
    + `<button type="button" class="btn small primary" id="go-coupon">쿠폰 화면으로</button></div>`;
  $('go-coupon').addEventListener('click', () => { showTab('coupon'); $('coupon-event-id').value = r.data.id; loadCouponEvent(r.data.id); });
  toast(`쿠폰 이벤트 #${r.data.id} 등록`, 'ok');
}

// ---------- 가상 사용자 (대기열 순번·매진 시연용) ----------
async function pool(items, size, fn) {
  let i = 0;
  const workers = Array.from({ length: Math.min(size, items.length) }, async () => {
    while (i < items.length) { const idx = i++; await fn(items[idx], idx); }
  });
  await Promise.all(workers);
}

function renderBots() {
  const b = S.bots;
  if (!b) return;
  const c = b.counts;
  $('bot-status').className = 'bot-status';
  $('bot-status').innerHTML = `<b>${esc(b.phase)}</b> · 로그인 <b>${c.login}</b>/${b.total} · 대기 <b>${c.waiting}</b> · 입장 <b>${c.admitted}</b>`
    + ` · 응모 성공 <b>${c.applied}</b> · 매진 <b>${c.soldOut}</b> · 기타 실패 <b>${c.failed}</b>`;
}

async function runBots(ev) {
  ev.preventDefault();
  if (S.bots && !S.bots.done) { toast('이미 진행 중입니다.', 'err'); return; }
  const dropId = Number($('f-bot-drop').value);
  const total = Number($('f-bot-count').value);
  const start = Number($('f-bot-start').value);
  const autoApply = $('f-bot-apply').checked;
  const dr = await api('GET', `/api/products/drops/${dropId}`, { auth: false });
  if (!dr.ok) { toast(errText(dr), 'err'); return; }
  const drop = dr.data;

  const b = S.bots = { total, done: false, stopped: false, phase: '로그인 중', counts: { login: 0, waiting: 0, admitted: 0, applied: 0, soldOut: 0, failed: 0 } };
  $('btn-bot-stop').disabled = false;
  renderBots();
  const users = Array.from({ length: total }, (_, k) => ({ email: testEmail(start + k), token: null, state: null }));
  const t0 = performance.now();
  await pool(users, 10, async (u) => {
    if (b.stopped) return;
    const r = await api('POST', '/api/users/login', { body: { email: u.email, password: TEST_PASSWORD }, auth: false, log: false });
    if (r.ok) { u.token = r.data.accessToken; b.counts.login++; } else b.counts.failed++;
    renderBots();
  });
  addNote(`가상 사용자 ${b.counts.login}명 로그인 (${Math.round(performance.now() - t0)}ms)`);
  const ready = users.filter((u) => u.token);

  // 오픈 전이면 오픈 시각까지 기다렸다가 한꺼번에 진입
  while (!b.stopped && Date.now() < Date.parse(drop.openAt) + 300) {
    b.phase = `오픈 대기 ${fmtDur(Date.parse(drop.openAt) - Date.now())}`;
    renderBots();
    await sleep(250);
  }
  if (b.stopped) { finishBots('중지됨'); return; }
  b.phase = '대기열 진입';
  renderBots();
  const t1 = performance.now();
  await pool(ready, 25, async (u) => {
    const r = await api('POST', `/api/queue/drops/${dropId}`, { token: u.token, log: false });
    if (r.ok) setBotState(u, r.data.state); else { u.state = 'FAILED'; b.counts.failed++; }
    renderBots();
  });
  addNote(`가상 사용자 ${ready.length}명 대기열 진입 요청 (${Math.round(performance.now() - t1)}ms)`);

  b.phase = '진행 중';
  // 각자 1초 주기로 상태 폴링 → 입장되면 응모
  await Promise.all(ready.map(async (u, k) => {
    await sleep((k * 37) % 1000);   // 폴링 시각을 흩어 둔다
    while (!b.stopped) {
      if (u.state === 'ADMITTED') {
        if (!autoApply) return;
        const r = await api('POST', `/api/entries/drops/${dropId}`, { token: u.token, log: false });
        b.counts.admitted--;
        if (r.ok) b.counts.applied++;
        else if (r.code === 'ENTRY_SOLD_OUT') b.counts.soldOut++;
        else b.counts.failed++;
        u.state = 'DONE';
        renderBots();
        return;
      }
      if (u.state !== 'WAITING') return;
      await sleep(QUEUE_POLL_MS);
      const r = await api('GET', `/api/queue/drops/${dropId}`, { token: u.token, log: false });
      if (r.ok) setBotState(u, r.data.state); else { setBotState(u, 'FAILED'); b.counts.failed++; }
      renderBots();
    }
  }));
  addNote(`가상 사용자 종료 — 응모 성공 ${b.counts.applied} · 매진 ${b.counts.soldOut} · 실패 ${b.counts.failed}`);
  finishBots(b.stopped ? '중지됨' : '완료');
}

function setBotState(u, state) {
  const c = S.bots.counts;
  if (u.state === 'WAITING') c.waiting--;
  if (u.state === 'ADMITTED') c.admitted--;
  u.state = state;
  if (state === 'WAITING') c.waiting++;
  else if (state === 'ADMITTED') c.admitted++;
  else if (state === 'SOLD_OUT') c.soldOut++;
}

function finishBots(phase) {
  S.bots.done = true;
  S.bots.phase = phase;
  $('btn-bot-stop').disabled = true;
  renderBots();
}

// ---------- 탭 ----------
function currentTab() { return document.querySelector('.tab.active').dataset.tab; }
function showTab(name) {
  document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.tab === name));
  document.querySelectorAll('.panel').forEach((p) => p.classList.toggle('hidden', p.id !== `tab-${name}`));
  store.set('qd.tab', name);
  if (name === 'ranking') loadRankings();
  if (name === 'drop') loadDrops();
}

// ---------- 주기 작업 ----------
let couponTick = 0;
function tick() {
  if (S.claims && S.claims.exp) $('session-exp').textContent = fmtDur(S.claims.exp * 1000 - Date.now());
  tickDrop();
  tickCoupon();
  // 쿠폰 탭에서 이벤트를 보고 있으면 2초마다 남은 수량 갱신 (API 로그에는 한 줄로 묶인다)
  if (++couponTick % 8 === 0 && S.coupon && currentTab() === 'coupon') loadCouponEvent(S.coupon.id);
}

// ---------- 이벤트 연결 ----------
function bind() {
  $('login-form').addEventListener('submit', (e) => { e.preventDefault(); login($('login-email').value.trim(), $('login-password').value); });
  $('btn-admin-login').addEventListener('click', () => login(ADMIN.email, ADMIN.password));
  $('btn-quick-login').addEventListener('click', () => login(testEmail(Number($('quick-user-no').value) || 1), TEST_PASSWORD));
  $('btn-signup').addEventListener('click', async () => {
    const email = $('login-email').value.trim();
    const password = $('login-password').value;
    const nickname = $('signup-nickname').value.trim();
    if (!email || !password || !nickname) { toast('이메일·비밀번호·닉네임을 모두 입력하세요.', 'err'); return; }
    const r = await api('POST', '/api/users/signup', { body: { email, password, nickname }, auth: false });
    if (!r.ok) { toast(errText(r), 'err'); return; }
    toast(`가입 완료 (#${r.data.userId})`, 'ok');
    login(email, password);
  });
  $('btn-logout').addEventListener('click', () => { logout(); toast('로그아웃했습니다.'); });

  document.querySelectorAll('.tab').forEach((t) => t.addEventListener('click', () => showTab(t.dataset.tab)));

  $('btn-drops-refresh').addEventListener('click', loadDrops);
  $('btn-queue-enter').addEventListener('click', enterQueue);
  $('btn-queue-stop').addEventListener('click', stopQueuePolling);
  $('btn-entry-apply').addEventListener('click', applyEntry);
  $('btn-entry-me').addEventListener('click', checkMyEntry);
  $('btn-draw-result').addEventListener('click', () => checkDrawResult(false));
  $('btn-my-entries').addEventListener('click', loadMyEntries);

  $('coupon-form').addEventListener('submit', (e) => { e.preventDefault(); loadCouponEvent(Number($('coupon-event-id').value)); });
  $('btn-coupon-issue').addEventListener('click', issueCoupon);
  $('btn-my-coupons').addEventListener('click', loadMyCoupons);

  $('btn-ranking-refresh').addEventListener('click', loadRankings);
  $('ranking-auto').addEventListener('change', (e) => setRankingAuto(e.target.checked));

  $('drop-form').addEventListener('submit', registerDrop);
  $('coupon-event-form').addEventListener('submit', registerCouponEvent);
  $('bot-form').addEventListener('submit', runBots);
  $('btn-bot-stop').addEventListener('click', () => { if (S.bots) S.bots.stopped = true; });

  $('btn-log-clear').addEventListener('click', () => { $('log-list').innerHTML = ''; lastLog = null; });
  $('btn-log-toggle').addEventListener('click', () => {
    const collapsed = $('log-panel').classList.toggle('collapsed');
    document.querySelector('.layout').classList.toggle('log-collapsed', collapsed);
    $('btn-log-toggle').textContent = collapsed ? '펼치기' : '접기';
  });
}

function init() {
  bind();
  S.claims = S.token ? decodeJwt(S.token) : null;
  if (S.claims && S.claims.exp * 1000 < Date.now()) setSession(null, null);
  renderSession();
  const tab = store.get('qd.tab') || 'drop';
  showTab(['drop', 'coupon', 'ranking', 'admin'].includes(tab) ? tab : 'drop');
  const dropId = Number(store.get('qd.drop'));
  if (dropId) selectDrop(dropId);
  const couponId = Number(store.get('qd.coupon'));
  if (couponId) { $('coupon-event-id').value = couponId; loadCouponEvent(couponId); }
  setInterval(tick, 250);
}

init();
