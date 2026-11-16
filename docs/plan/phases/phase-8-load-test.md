# 8단계 — 검증 (부하 테스트) 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 트래픽이 몰려도 **정원·수량 초과 0, 중복 0** 임을 수치로 증명하고, 처리량·지연·병목을 리포트로 남긴다 |
| 요구사항 | NFR-01 (초과 없음), NFR-02 (중복 없음), NFR-03 (Redis·Kafka·DB 정합성), REQ-QUEUE-04·05, REQ-ENTRY-02·03, REQ-COUPON-02·03, REQ-RANK-03 |
| 브랜치 | `test/phase-8-load-test` |
| 선행 조건 | 0~7단계 완료 (모든 서비스 기능, Redis AOF·토픽 초기화가 반영된 docker-compose) |
| 참고 설계 | [infrastructure.md](../../design/03-infrastructure/infrastructure.md) (3~6장), [architecture.md](../../design/02-architecture/architecture.md) 6장(내부 포트 비노출), [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md), [queue-enter.md](../../design/05-features/queue-enter.md), [entry-apply.md](../../design/05-features/entry-apply.md), [coupon-issue.md](../../design/05-features/coupon-issue.md), [draw.md](../../design/05-features/draw.md) |

**이 단계에서 증명할 것**
1. 정원 M, 사용자 N(≫M) 이 동시에 몰려도 DB 응모 수 ≤ M, 사용자당 1건, 응모 번호 중복 0.
2. 성공 응답(202) 수 == DB 저장 수 == Redis 응모자 수 (컨슈머 지연이 해소된 뒤).
3. 쿠폰도 같은 조건에서 발급 수 ≤ 수량, 사용자당 1매.
4. 처리량(TPS), 지연(p95/p99), 에러율, Kafka consumer lag 과 그 해소 시간.

---

## 2. 산출물 파일 목록

```
queue-drop/
├── build.gradle                                   [수정] plain jar 비활성, Prometheus 레지스트리 의존성
├── .dockerignore                                  [신규]
├── docker/
│   └── service.Dockerfile                         [신규] 모든 서비스 공용 (ARG MODULE)
├── docker-compose.yml                             [수정] app / loadtest / monitoring 프로필 추가
├── infra/
│   └── monitoring/prometheus.yml                  [신규] (선택)
├── {gateway,user,product,queue,entry,coupon,ranking}-service/
│   └── src/main/resources/application.yml         [수정] Actuator·메트릭, 튜닝 파라미터 환경변수화
├── load-test/
│   ├── lib/config.js                              [신규] 공통 설정·헤더
│   ├── lib/api.js                                 [신규] 로그인·관리자 API·대기
│   ├── prepare-users.js                           [신규] 테스트 사용자 사전 생성
│   ├── drop-open-spike.js                         [신규] 시나리오 S1·S2
│   └── coupon-spike.js                            [신규] 시나리오 S3·S4
├── scripts/
│   ├── verify-drop.sh                             [신규] 응모·추첨 정합성 검증
│   ├── verify-coupon.sh                           [신규] 쿠폰 정합성 검증
│   └── kafka-lag.sh                               [신규] 컨슈머 그룹 lag 조회
└── docs/report/
    ├── load-test-report.md                        [신규] 리포트 (템플릿 → 결과 기록)
    └── results/                                   [신규] k6 결과 JSON, 검증 스크립트 출력 (.gitkeep)
```
> gateway 모듈은 디렉터리 이름이 `gateway` 이다 (`gateway-service` 아님).

---

## 3. 구현 상세

### 3.1 빌드 · 컨테이너 이미지

#### 방식: 호스트에서 `bootJar` → 공용 Dockerfile 이 jar 만 복사 (구현 결정)
| 선택지 | 장점 | 단점 |
|--------|------|------|
| 멀티 스테이지 (컨테이너 안에서 Gradle 빌드) | 호스트 JDK 불필요, 재현성 | 이미지마다 Gradle 의존성 다운로드 → 7개 서비스 빌드가 매우 느림 |
| **bootJar 기반 (채택)** | 빌드는 Gradle 캐시를 쓰는 호스트에서 한 번, 이미지 빌드는 수 초 | 호스트에 JDK 21 필요 (이미 있음) |

부하 테스트는 "코드 수정 → 재빌드 → 재측정"을 반복하므로 반복 속도를 우선했다.

#### 루트 `build.gradle` 수정분
```groovy
configure(subprojects.findAll { it.name != 'common' }) {
    apply plugin: 'org.springframework.boot'

    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        runtimeOnly 'io.micrometer:micrometer-registry-prometheus'       // 추가
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
    }

    tasks.named('jar') { enabled = false }   // 추가: -plain.jar 생성 안 함 → build/libs 에 jar 1개
}
```

#### `docker/service.Dockerfile`
```dockerfile
# 모든 서비스 공용. 사용: docker build -f docker/service.Dockerfile --build-arg MODULE=entry-service .
FROM eclipse-temurin:21-jre

ARG MODULE
# healthcheck 용 curl (베이스 이미지 포함 여부에 의존하지 않음)
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app

WORKDIR /app
COPY ${MODULE}/build/libs/*.jar app.jar
USER app

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
```
> 베이스 이미지 태그 `eclipse-temurin:21-jre` 는 구현 시 확인 (Ubuntu 기반이라 `apt-get` 사용).

#### `.dockerignore`
```
.git
.gradle
.idea
**/build/tmp
**/build/classes
**/build/resources
**/src
docs
load-test
node_modules
```
> `**/build/libs` 는 제외하지 않는다 (jar 를 복사해야 하므로).

### 3.2 Actuator · 메트릭

#### 각 서비스 `application.yml` 공통 추가분 (gateway 포함 7개)
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      probes:
        enabled: true
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        http.server.requests: true
      percentiles:
        http.server.requests: 0.5,0.95,0.99
```

#### 관찰할 메트릭
| 메트릭 | 출처 | 의미 |
|--------|------|------|
| `http.server.requests` (uri, status 태그) | Spring 기본 | 서비스 측 지연 분포 (k6 측정값과 비교 → 네트워크·Gateway 비용 분리) |
| `kafka.consumer.fetch.manager.records.lag.max` | Spring Kafka + Micrometer 자동 바인딩 (구현 시 확인) | 컨슈머 지연 |
| `entry.apply.outcome` (outcome 태그) | 커스텀 Counter (구현 결정) | OK / SOLD_OUT / DUPLICATE / NO_TOKEN / CLOSED / TEMPORARILY_FAILED 분포 |
| `entry.apply.compensated` | 커스텀 Counter (구현 결정) | 보상 Lua 실행 횟수 (0 이 정상) |
| `coupon.issue.outcome` (outcome 태그) | 커스텀 Counter (구현 결정) | 쿠폰 판정 분포 |
| `queue.admitted` | 커스텀 Counter (구현 결정) | 입장 토큰 발급 수 |
| `hikaricp.connections.pending` | Spring 기본 | DB 커넥션 대기 (컨슈머 저장 병목 확인) |
| `jvm.gc.pause`, `process.cpu.usage` | Spring 기본 | 리소스 병목 |

> **커스텀 메트릭 위치 (구현 결정)**: application 계층은 Micrometer 에 의존하지 않는다. 판정 결과 카운트는 presentation(Controller/ExceptionHandler)에서, 보상·입장 카운트는 infrastructure 어댑터(`RedisEntryGate.compensate`, `RedisWaitingQueueRepository.admit`)에서 기록한다.

#### 튜닝 파라미터 환경변수화
부하 테스트 중 재빌드 없이 바꿀 값들. 프로퍼티 이름은 3·4·5단계 구현 상세 문서의 이름과 반드시 맞춘다 (아래는 Spring relaxed binding 기준 가정 — 구현 결정).

| 서비스 | 프로퍼티 (가정) | 환경변수 | 기본값 |
|--------|-----------------|----------|--------|
| queue | `queue.admission.batch-size` | `QUEUE_ADMISSION_BATCH_SIZE` | 100 |
| queue | `queue.admission.interval-ms` | `QUEUE_ADMISSION_INTERVAL_MS` | 1000 |
| queue | `queue.token.ttl` | `QUEUE_TOKEN_TTL` | 300s |
| entry | `entry.publish.timeout` | `ENTRY_PUBLISH_TIMEOUT` | 3s |
| entry | `entry.publish.retries` | `ENTRY_PUBLISH_RETRIES` | 2 |
| entry | `entry.draw.interval-ms` | `ENTRY_DRAW_INTERVAL_MS` | 5000 |
| coupon | `coupon.publish.timeout` | `COUPON_PUBLISH_TIMEOUT` | 3s |
| user | `jwt.expiration` | `JWT_EXPIRATION` | 부하 테스트 시 2h |
| 공통 | `spring.kafka.listener.concurrency` | `SPRING_KAFKA_LISTENER_CONCURRENCY` | 1 (파티션 수 이하로 조정) |
| 공통 | `spring.datasource.hikari.maximum-pool-size` | `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` | 10 |

### 3.3 docker-compose `app` / `loadtest` / `monitoring` 프로필

인프라(postgres, redis, kafka, kafka 토픽 초기화, kafka-ui)는 0단계 구성 그대로 두고, **프로필이 붙은 서비스만 추가**한다.
- `docker compose up -d` → 인프라만 (기존 개발 방식 유지)
- `docker compose --profile app up -d --build` → 인프라 + 서비스 7개
- `docker compose --profile loadtest run --rm k6 run /scripts/...` → k6 실행
- `docker compose --profile monitoring up -d` → Prometheus + Grafana (선택)

```yaml
# docker-compose.yml 추가분 (기존 services: 아래에 이어서)

x-app-common: &app-common
  build:
    context: .
    dockerfile: docker/service.Dockerfile
  profiles: ["app"]
  restart: unless-stopped
  deploy:
    resources:
      limits:
        memory: 768m        # 측정 환경에 맞게 조정 (리포트에 기록)
  healthcheck:
    interval: 5s
    timeout: 3s
    retries: 30

x-db-env: &db-env
  DB_HOST: postgres
  DB_PORT: 5432
  DB_USERNAME: queuedrop
  DB_PASSWORD: queuedrop

x-redis-env: &redis-env
  REDIS_HOST: redis
  REDIS_PORT: 6379

x-kafka-env: &kafka-env
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092     # 컨테이너 간에는 INTERNAL 리스너

x-infra-deps: &infra-deps
  postgres: { condition: service_healthy }
  redis: { condition: service_healthy }
  kafka: { condition: service_started }

services:
  # ... 기존 인프라 서비스 (변경 없음) ...

  gateway:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: gateway } }
    container_name: queuedrop-gateway
    ports:
      - "8080:8080"                         # 외부에 노출되는 유일한 애플리케이션 포트
    environment:
      JWT_SECRET: ${JWT_SECRET:?JWT_SECRET 필요}
      USER_SERVICE_URL: http://user-service:8081
      PRODUCT_SERVICE_URL: http://product-service:8082
      QUEUE_SERVICE_URL: http://queue-service:8083
      COUPON_SERVICE_URL: http://coupon-service:8084
      ENTRY_SERVICE_URL: http://entry-service:8085
      RANKING_SERVICE_URL: http://ranking-service:8086
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8080/actuator/health"]
    depends_on:
      user-service: { condition: service_healthy }
      product-service: { condition: service_healthy }
      queue-service: { condition: service_healthy }
      entry-service: { condition: service_healthy }
      coupon-service: { condition: service_healthy }
      ranking-service: { condition: service_healthy }

  user-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: user-service } }
    container_name: queuedrop-user
    # ports 없음: 내부 네트워크에서만 접근
    environment:
      <<: [*db-env, *kafka-env]
      JWT_SECRET: ${JWT_SECRET:?JWT_SECRET 필요}
      JWT_EXPIRATION: ${JWT_EXPIRATION:-2h}
      ADMIN_EMAIL: ${ADMIN_EMAIL:-admin@queuedrop.test}         # 6장 "설계 수정 필요" 참고
      ADMIN_PASSWORD: ${ADMIN_PASSWORD:-Admin!2345}
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8081/actuator/health"]
    depends_on: *infra-deps

  product-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: product-service } }
    container_name: queuedrop-product
    environment:
      <<: [*db-env, *kafka-env]
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8082/actuator/health"]
    depends_on: *infra-deps

  queue-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: queue-service } }
    container_name: queuedrop-queue
    environment:
      <<: [*redis-env, *kafka-env]
      QUEUE_ADMISSION_BATCH_SIZE: ${QUEUE_ADMISSION_BATCH_SIZE:-100}
      QUEUE_ADMISSION_INTERVAL_MS: ${QUEUE_ADMISSION_INTERVAL_MS:-1000}
      QUEUE_TOKEN_TTL: ${QUEUE_TOKEN_TTL:-300s}
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8083/actuator/health"]
    depends_on: *infra-deps

  coupon-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: coupon-service } }
    container_name: queuedrop-coupon
    environment:
      <<: [*db-env, *redis-env, *kafka-env]
      COUPON_PUBLISH_TIMEOUT: ${COUPON_PUBLISH_TIMEOUT:-3s}
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8084/actuator/health"]
    depends_on: *infra-deps

  entry-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: entry-service } }
    container_name: queuedrop-entry
    environment:
      <<: [*db-env, *redis-env, *kafka-env]
      ENTRY_PUBLISH_TIMEOUT: ${ENTRY_PUBLISH_TIMEOUT:-3s}
      ENTRY_PUBLISH_RETRIES: ${ENTRY_PUBLISH_RETRIES:-2}
      ENTRY_DRAW_INTERVAL_MS: ${ENTRY_DRAW_INTERVAL_MS:-5000}
      SPRING_KAFKA_LISTENER_CONCURRENCY: ${ENTRY_LISTENER_CONCURRENCY:-3}
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8085/actuator/health"]
    depends_on: *infra-deps

  ranking-service:
    <<: *app-common
    build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: ranking-service } }
    container_name: queuedrop-ranking
    environment:
      <<: [*redis-env, *kafka-env]
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:8086/actuator/health"]
    depends_on:
      redis: { condition: service_healthy }
      kafka: { condition: service_started }

  k6:
    image: grafana/k6:0.54.0                 # 태그는 구현 시 확인
    profiles: ["loadtest"]
    environment:
      BASE_URL: http://gateway:8080
      ADMIN_EMAIL: ${ADMIN_EMAIL:-admin@queuedrop.test}
      ADMIN_PASSWORD: ${ADMIN_PASSWORD:-Admin!2345}
    volumes:
      - ./load-test:/scripts:ro
      - ./docs/report/results:/results         # k6 이미지는 비루트 사용자 → 쓰기 권한 확인 (6장)

  prometheus:                                  # (선택)
    image: prom/prometheus:v2.53.0              # 태그는 구현 시 확인
    profiles: ["monitoring"]
    volumes:
      - ./infra/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"

  grafana:                                     # (선택)
    image: grafana/grafana:11.1.0               # 태그는 구현 시 확인
    profiles: ["monitoring"]
    ports:
      - "3000:3000"
    depends_on: [prometheus]
```
- `build:` 를 서비스마다 다시 쓰는 이유: YAML 병합(`<<`)은 얕은 병합이라 `args` 만 덮어쓸 수 없다.
- 인프라 컨테이너(postgres 5432, redis 6379, kafka 9092)는 검증 스크립트와 로컬 개발을 위해 호스트 포트를 유지한다. **애플리케이션 서비스는 gateway 만** 노출한다 ([architecture.md](../../design/02-architecture/architecture.md) 6장 "Gateway 우회 방지").
- `deploy.resources.limits` 는 Compose v2 비-Swarm 모드에서도 적용된다 (구현 시 `docker stats` 로 확인).

#### `infra/monitoring/prometheus.yml` (선택)
```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: queue-drop
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - gateway:8080
          - user-service:8081
          - product-service:8082
          - queue-service:8083
          - coupon-service:8084
          - entry-service:8085
          - ranking-service:8086
```

#### 실행 순서
```bash
export JWT_SECRET=$(openssl rand -base64 48)
./gradlew clean bootJar
docker compose --profile app up -d --build
docker compose ps                                  # 전부 healthy 확인
curl -fs localhost:8080/actuator/health            # gateway OK
curl -s  localhost:8085/actuator/health || echo "내부 포트 비노출 확인"
```

### 3.4 k6 스크립트

#### 설계 원칙 (구현 결정)
- **사용자 1명 = VU 1개** (`per-vu-iterations`, iterations=1). 대기열은 "진입 → 폴링 → 응모"가 한 사람의 순차 흐름이라 도착률 기반 실행기보다 자연스럽다.
- **로그인은 오픈 전에 끝낸다.** 각 VU 가 시작하자마자 로그인하고, 드로우 오픈 시각까지 대기한 뒤 동시에 진입한다. "페이지를 열어두고 오픈을 기다리던 사람들"을 흉내 내고, BCrypt 로그인 부하가 측정 구간에 섞이지 않게 한다. 로그인 요청은 `phase:prepare` 태그로 분리해 임계값에서 제외한다.
- **사용자는 사전 생성**한다 (`prepare-users.js`). 이메일은 `loadtest-{i}@queuedrop.test` 규칙으로 결정적이라 토큰을 파일로 주고받을 필요가 없다.
- **409 는 정상 응답**으로 본다 (SOLD_OUT, DUPLICATE 등은 의도된 결과). 5xx 와 네트워크 오류만 실패로 집계한다.
- 응답 필드 이름(`accessToken`, `id`, `state`, `nextPollAfterMs`, `code`)은 1~6단계 API 와 맞춘다 (구현 시 확인).

#### `load-test/lib/config.js`
```javascript
export const BASE_URL = __ENV.BASE_URL || 'http://gateway:8080';
export const USER_PASSWORD = __ENV.USER_PASSWORD || 'LoadTest!234';
export const ADMIN_EMAIL = __ENV.ADMIN_EMAIL || 'admin@queuedrop.test';
export const ADMIN_PASSWORD = __ENV.ADMIN_PASSWORD || 'Admin!2345';

export const userEmail = (i) => `loadtest-${i}@queuedrop.test`;

export const JSON = { 'Content-Type': 'application/json' };

export function auth(token, tags) {
  return { headers: { ...JSON, Authorization: `Bearer ${token}` }, tags };
}

export const int = (name, def) => parseInt(__ENV[name] || `${def}`, 10);
```

#### `load-test/lib/api.js`
```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { BASE_URL, JSON as H, auth } from './config.js';

export function login(email, password, tags = { name: 'login', phase: 'prepare' }) {
  const res = http.post(`${BASE_URL}/api/users/login`,
    JSON.stringify({ email, password }), { headers: H, tags });
  check(res, { 'login 200': (r) => r.status === 200 });
  return res.status === 200 ? res.json('accessToken') : null;
}

export function createProduct(adminToken, name) {
  const res = http.post(`${BASE_URL}/api/products`,
    JSON.stringify({ name, description: 'load test', imageUrl: null }),
    auth(adminToken, { name: 'admin', phase: 'setup' }));
  check(res, { 'product 201': (r) => r.status === 201 });
  return res.json('id');
}

export function createDrop(adminToken, productId, d) {
  const res = http.post(`${BASE_URL}/api/products/${productId}/drops`,
    JSON.stringify(d), auth(adminToken, { name: 'admin', phase: 'setup' }));
  check(res, { 'drop 201': (r) => r.status === 201 });
  return res.json('id');
}

export function createCouponEvent(adminToken, e) {
  const res = http.post(`${BASE_URL}/api/coupons/events`,
    JSON.stringify(e), auth(adminToken, { name: 'admin', phase: 'setup' }));
  check(res, { 'coupon event 201': (r) => r.status === 201 });
  return res.json('id');
}

/** 목표 시각(epoch ms)까지 대기. jitterMs 만큼 무작위로 늦게 출발 (사람마다 클릭 시점이 다름) */
export function waitUntil(epochMs, jitterMs = 0) {
  const ms = epochMs - Date.now() + Math.random() * jitterMs;
  if (ms > 0) sleep(ms / 1000);
}

export const errorCode = (res) => {
  try { return res.json('code'); } catch (e) { return 'UNKNOWN'; }
};
```

#### `load-test/prepare-users.js` — 테스트 사용자 사전 생성 (1회)
```javascript
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { BASE_URL, JSON as H, USER_PASSWORD, userEmail, int } from './lib/config.js';

const TOTAL = int('TOTAL_USERS', 5200);   // S1~S4 에서 쓰는 최대 사용자 수

http.setResponseCallback(http.expectedStatuses(201, 409));   // 409 = 이미 존재 (재실행 허용)

export const options = {
  scenarios: {
    prepare: { executor: 'shared-iterations', vus: int('PREPARE_VUS', 50), iterations: TOTAL, maxDuration: '20m' },
  },
};

export default function () {
  const i = exec.scenario.iterationInTest + 1;          // 1..TOTAL
  const res = http.post(`${BASE_URL}/api/users/signup`,
    JSON.stringify({ email: userEmail(i), password: USER_PASSWORD, nickname: `lt-${i}` }),
    { headers: H, tags: { name: 'signup', phase: 'prepare' } });
  check(res, { 'created or exists': (r) => r.status === 201 || r.status === 409 });
}
```
실행: `docker compose --profile loadtest run --rm k6 run -e TOTAL_USERS=5200 /scripts/prepare-users.js`

#### `load-test/drop-open-spike.js` — 시나리오 S1·S2
```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';  // 구현 시 확인
import { BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD, USER_PASSWORD, userEmail, auth, int } from './lib/config.js';
import { login, createProduct, createDrop, waitUntil, errorCode } from './lib/api.js';

const USERS = int('USERS', 3000);
const CAPACITY = int('CAPACITY', 1000);          // 응모 정원 M
const WINNERS = int('WINNERS', 100);             // 당첨 수량 K
const OPEN_DELAY_SEC = int('OPEN_DELAY_SEC', 90);    // 드로우 등록 → 오픈 (설계: 최소 1분 권장)
const ENTRY_WINDOW_SEC = int('ENTRY_WINDOW_SEC', 300);
const JITTER_MS = int('JITTER_MS', 1000);
const POLL_MS = int('POLL_MS', 1000);
const MAX_WAIT_SEC = int('MAX_WAIT_SEC', 600);

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409));

const queueWait = new Trend('queue_wait_ms', true);         // 진입 → 입장까지 걸린 시간
const entryOk = new Counter('entry_ok');
const entryRejected = new Counter('entry_rejected');        // tag: code
const entryRetryable = new Counter('entry_retryable_503');
const queueSoldOut = new Counter('queue_sold_out');
const queueLost = new Counter('queue_lost');                // 토큰 만료 등으로 NOT_IN_QUEUE
const queueTimeout = new Counter('queue_timeout');
const unexpected = new Counter('unexpected');

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
  },
};

export function setup() {
  const admin = login(ADMIN_EMAIL, ADMIN_PASSWORD, { name: 'admin', phase: 'setup' });
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
  const i = exec.vu.idInTest;                               // 1..USERS
  const token = login(userEmail(i), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }

  waitUntil(data.openAtMs, JITTER_MS);

  // 1) 대기열 진입
  const tags = (name) => auth(token, { name, phase: 'open' });
  let res = http.post(`${BASE_URL}/api/queue/drops/${data.dropId}`, null, tags('queue_enter'));
  if (res.status === 409) {
    const code = errorCode(res);
    if (code === 'NOT_OPEN') {                              // 시계 차이로 살짝 이르게 도착
      sleep(1);
      res = http.post(`${BASE_URL}/api/queue/drops/${data.dropId}`, null, tags('queue_enter'));
    } else { queueSoldOut.add(1, { code }); return; }
  }
  if (res.status !== 200) { unexpected.add(1, { step: 'enter', status: `${res.status}` }); return; }

  // 2) 폴링
  let state = res.json('state');
  const enteredAt = Date.now();
  while (state === 'WAITING' && Date.now() - enteredAt < MAX_WAIT_SEC * 1000) {
    sleep((res.json('nextPollAfterMs') || POLL_MS) / 1000);
    res = http.get(`${BASE_URL}/api/queue/drops/${data.dropId}`, tags('queue_status'));
    if (res.status !== 200) { unexpected.add(1, { step: 'status', status: `${res.status}` }); return; }
    state = res.json('state');
  }

  if (state === 'SOLD_OUT') { queueSoldOut.add(1); return; }
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
    unexpected.add(1, { step: 'apply', status: `${apply.status}` });
    return;
  }
}

export function handleSummary(data) {
  const dropId = data.setup_data ? data.setup_data.dropId : 'unknown';   // setup_data 노출 여부 구현 시 확인
  const name = `drop-open-${dropId}-${Date.now()}`;
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    [`/results/${name}.json`]: JSON.stringify(data, null, 2),
  };
}
```
실행 예:
```bash
# S1: 기본 — 사용자 3,000 / 정원 1,000
docker compose --profile loadtest run --rm k6 run \
  -e USERS=3000 -e CAPACITY=1000 -e WINNERS=100 /scripts/drop-open-spike.js
# S2: 정원 소진 위주 — 사용자 5,000 / 정원 100
docker compose --profile loadtest run --rm k6 run \
  -e USERS=5000 -e CAPACITY=100 -e WINNERS=10 /scripts/drop-open-spike.js
```

#### `load-test/coupon-spike.js` — 시나리오 S3(스파이크)·S4(같은 사용자 중복 버스트)
```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';  // 구현 시 확인
import { BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD, USER_PASSWORD, userEmail, auth, int } from './lib/config.js';
import { login, createCouponEvent, waitUntil, errorCode } from './lib/api.js';

const USERS = int('USERS', 3000);
const QUANTITY = int('QUANTITY', 500);
const DUP_USERS = int('DUP_USERS', 200);        // S4 참여 사용자 수 (USERS 다음 번호 사용)
const BURST = int('BURST', 5);                  // 한 사용자의 동시 요청 수
const START_DELAY_SEC = int('START_DELAY_SEC', 60);
const JITTER_MS = int('JITTER_MS', 500);

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409));

const couponOk = new Counter('coupon_ok');
const couponRejected = new Counter('coupon_rejected');     // tag: code
const couponRetryable = new Counter('coupon_retryable_503');
const dupViolation = new Counter('coupon_dup_violation');  // 한 사용자 버스트에서 202 가 2개 이상
const unexpected = new Counter('unexpected');

export const options = {
  setupTimeout: '2m',
  scenarios: {
    spike: { executor: 'per-vu-iterations', vus: USERS, iterations: 1, exec: 'spike', maxDuration: '10m' },
    dup_burst: { executor: 'per-vu-iterations', vus: DUP_USERS, iterations: 1, exec: 'dupBurst', maxDuration: '10m' },
  },
  thresholds: {
    'http_req_duration{name:coupon_issue}': ['p(95)<200', 'p(99)<500'],
    'http_req_failed{phase:open}': ['rate<0.001'],
    coupon_dup_violation: ['count==0'],
    unexpected: ['count==0'],
  },
};

export function setup() {
  const admin = login(ADMIN_EMAIL, ADMIN_PASSWORD, { name: 'admin', phase: 'setup' });
  const startAtMs = Date.now() + START_DELAY_SEC * 1000;
  const eventId = createCouponEvent(admin, {
    name: `coupon-spike-${QUANTITY}`,
    totalQuantity: QUANTITY,
    issueStartAt: new Date(startAtMs).toISOString(),
  });
  console.log(`couponEventId=${eventId}`);
  return { eventId, startAtMs };
}

function issue(token, eventId) {
  return http.post(`${BASE_URL}/api/coupons/events/${eventId}/issue`, null,
    auth(token, { name: 'coupon_issue', phase: 'open' }));
}

function record(res) {
  if (res.status === 202) { couponOk.add(1); return 1; }
  if (res.status === 409) { couponRejected.add(1, { code: errorCode(res) }); return 0; }
  if (res.status === 503) { couponRetryable.add(1); return 0; }
  unexpected.add(1, { status: `${res.status}` });
  return 0;
}

export function spike(data) {
  const token = login(userEmail(exec.vu.idInTest), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }
  waitUntil(data.startAtMs, JITTER_MS);
  record(issue(token, data.eventId));
}

export function dupBurst(data) {
  // spike 와 겹치지 않게 USERS 이후 번호 사용 → prepare 시 USERS + DUP_USERS 명 필요
  const token = login(userEmail(USERS + exec.vu.idInScenario), USER_PASSWORD);
  if (!token) { unexpected.add(1, { step: 'login' }); return; }
  waitUntil(data.startAtMs, JITTER_MS);
  const reqs = Array.from({ length: BURST }, () => ({
    method: 'POST',
    url: `${BASE_URL}/api/coupons/events/${data.eventId}/issue`,
    body: null,
    params: auth(token, { name: 'coupon_issue_burst', phase: 'open' }),
  }));
  const ok = http.batch(reqs).reduce((sum, r) => sum + record(r), 0);
  check(ok, { '버스트 중 최대 1매': (n) => n <= 1 });
  if (ok > 1) dupViolation.add(1);
}

export function handleSummary(data) {
  const id = data.setup_data ? data.setup_data.eventId : 'unknown';
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    [`/results/coupon-${id}-${Date.now()}.json`]: JSON.stringify(data, null, 2),
  };
}
```

### 3.5 결과 검증 스크립트

#### `scripts/verify-drop.sh`
```bash
#!/usr/bin/env bash
# 사용: scripts/verify-drop.sh <dropId> [--after-draw] [--wait-sec 60]
set -euo pipefail
DROP_ID=$1; shift
AFTER_DRAW=false; WAIT_SEC=60
while [[ $# -gt 0 ]]; do
  case $1 in --after-draw) AFTER_DRAW=true ;; --wait-sec) WAIT_SEC=$2; shift ;; esac; shift
done

sql()   { docker compose exec -T postgres psql -U queuedrop -d entry_db -At -c "$1"; }
redis() { docker compose exec -T redis redis-cli "$@" | tr -d '\r'; }
FAIL=0
assert() { if [[ "$2" == "true" ]]; then echo "PASS  $1"; else echo "FAIL  $1"; FAIL=1; fi; }

CAPACITY=$(sql "SELECT entry_capacity FROM drop_snapshots WHERE drop_id=$DROP_ID")
WINNERS=$(sql "SELECT winner_count FROM drop_snapshots WHERE drop_id=$DROP_ID")
REMAINING=$(redis GET "entry:$DROP_ID:remaining")
APPLICANTS=$(redis SCARD "entry:$DROP_ID:applicants")

# 컨슈머 지연 해소 대기: DB 응모 수가 Redis 응모자 수에 도달할 때까지
for ((s=0; s<WAIT_SEC; s++)); do
  DB_COUNT=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID")
  [[ "$DB_COUNT" == "$APPLICANTS" ]] && break; sleep 1
done
DUP_USER=$(sql "SELECT COUNT(*) FROM (SELECT user_id FROM entries WHERE drop_id=$DROP_ID GROUP BY user_id HAVING COUNT(*)>1) t")
DUP_SEQ=$(sql "SELECT COUNT(*) FROM (SELECT entry_seq FROM entries WHERE drop_id=$DROP_ID GROUP BY entry_seq HAVING COUNT(*)>1) t")
POPULAR=$(redis ZSCORE rank:drop:popular "$DROP_ID"); POPULAR=${POPULAR:-0}

echo "capacity=$CAPACITY remaining=$REMAINING redisApplicants=$APPLICANTS db=$DB_COUNT rankingScore=$POPULAR"
assert "NFR-01 DB 응모 수 ≤ 정원"                  "$([[ $DB_COUNT -le $CAPACITY ]] && echo true)"
assert "NFR-01 Redis 응모자 수 ≤ 정원"             "$([[ $APPLICANTS -le $CAPACITY ]] && echo true)"
assert "NFR-02 사용자 중복 0"                      "$([[ $DUP_USER -eq 0 ]] && echo true)"
assert "NFR-02 응모 번호 중복 0"                   "$([[ $DUP_SEQ -eq 0 ]] && echo true)"
assert "NFR-03 정원 - 잔여 == Redis 응모자 수"     "$([[ $((CAPACITY - REMAINING)) -eq $APPLICANTS ]] && echo true)"
assert "NFR-03 Redis 응모자 수 == DB 응모 수"       "$([[ $APPLICANTS -eq $DB_COUNT ]] && echo true)"
assert "REQ-RANK-03 인기 랭킹 점수 == DB 응모 수"   "$([[ ${POPULAR%.*} -eq $DB_COUNT ]] && echo true)"

if $AFTER_DRAW; then
  STATUS=$(sql "SELECT status FROM drop_snapshots WHERE drop_id=$DROP_ID")
  WON=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID AND status='WON'")
  APPLIED=$(sql "SELECT COUNT(*) FROM entries WHERE drop_id=$DROP_ID AND status='APPLIED'")
  RESULTS=$(sql "SELECT COUNT(*) FROM draw_results WHERE drop_id=$DROP_ID")
  EXPECTED=$(( DB_COUNT < WINNERS ? DB_COUNT : WINNERS ))
  assert "추첨 상태 DRAWN"                       "$([[ $STATUS == DRAWN ]] && echo true)"
  assert "당첨 수 == min(K, 응모자 수)"           "$([[ $WON -eq $EXPECTED ]] && echo true)"
  assert "미처리(APPLIED) 0"                     "$([[ $APPLIED -eq 0 ]] && echo true)"
  assert "draw_results 1건"                      "$([[ $RESULTS -eq 1 ]] && echo true)"
fi
exit $FAIL
```
> k6 요약의 `entry_ok` 카운트(성공 응답 수)도 DB 응모 수와 같아야 한다. 스크립트 출력과 k6 결과 JSON 을 리포트에서 함께 대조한다.

#### `scripts/verify-coupon.sh`
```bash
#!/usr/bin/env bash
# 사용: scripts/verify-coupon.sh <couponEventId> [--wait-sec 60]
set -euo pipefail
EVENT_ID=$1; WAIT_SEC=${3:-60}
sql()   { docker compose exec -T postgres psql -U queuedrop -d coupon_db -At -c "$1"; }
redis() { docker compose exec -T redis redis-cli "$@" | tr -d '\r'; }
FAIL=0
assert() { if [[ "$2" == "true" ]]; then echo "PASS  $1"; else echo "FAIL  $1"; FAIL=1; fi; }

TOTAL=$(sql "SELECT total_quantity FROM coupon_events WHERE id=$EVENT_ID")
REMAINING=$(redis GET "coupon:$EVENT_ID:remaining")
ISSUED=$(redis SCARD "coupon:$EVENT_ID:issued")
for ((s=0; s<WAIT_SEC; s++)); do
  DB_COUNT=$(sql "SELECT COUNT(*) FROM issued_coupons WHERE coupon_event_id=$EVENT_ID")
  [[ "$DB_COUNT" == "$ISSUED" ]] && break; sleep 1
done
DUP=$(sql "SELECT COUNT(*) FROM (SELECT user_id FROM issued_coupons WHERE coupon_event_id=$EVENT_ID GROUP BY user_id HAVING COUNT(*)>1) t")

echo "total=$TOTAL remaining=$REMAINING redisIssued=$ISSUED db=$DB_COUNT"
assert "NFR-01 발급 수 ≤ 총 수량"              "$([[ $DB_COUNT -le $TOTAL ]] && echo true)"
assert "NFR-02 사용자 중복 0"                  "$([[ $DUP -eq 0 ]] && echo true)"
assert "NFR-03 총 수량 - 잔여 == Redis 발급 수" "$([[ $((TOTAL - REMAINING)) -eq $ISSUED ]] && echo true)"
assert "NFR-03 Redis 발급 수 == DB 발급 수"     "$([[ $ISSUED -eq $DB_COUNT ]] && echo true)"
exit $FAIL
```

#### `scripts/kafka-lag.sh`
```bash
#!/usr/bin/env bash
# 사용: scripts/kafka-lag.sh [group ...]   (기본: 응모·쿠폰·랭킹 컨슈머)
set -euo pipefail
TARGETS=${*:-"entry-service coupon-service ranking-service"}   # GROUPS 는 bash 예약 변수라 사용 금지
for g in $TARGETS; do
  echo "== $g"
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group "$g"
done
```
> 컨슈머 그룹 이름은 각 서비스 `application.yml` 의 `group-id` (`entry-service`, `coupon-service`, `ranking-service` 등)와 같다.
> **lag 해소 시간** 측정: 부하 종료 시각부터 `LAG` 합계가 0 이 될 때까지 `watch -n 1 scripts/kafka-lag.sh entry-service` 로 관찰해 기록한다.

### 3.6 측정 시나리오 · 목표

| ID | 시나리오 | 파라미터 (기본) | 관찰 지표 | 초기 목표 (구현 결정) |
|----|----------|-----------------|-----------|------------------------|
| S1 | 드로우 오픈 스파이크 | USERS 3,000 / M 1,000 / K 100 / 배치 100·1초 | 진입·응모 TPS, `queue_enter`·`entry_apply` p95/p99, 5xx 비율, `queue_wait_ms`, entry-service lag 최대값·해소 시간 | 응모 p99 < 500ms, 5xx < 0.1%, lag 30초 내 해소, **초과·중복 0** |
| S2 | 정원 조기 소진 | USERS 5,000 / M 100 / K 10 | `entry_rejected{code:SOLD_OUT}`, `queue_sold_out`, 소진 후 입장 중단 여부 | 성공 정확히 100, 소진 이후 토큰 발급 중단 |
| S3 | 쿠폰 스파이크 | USERS 3,000 / 수량 500 | `coupon_issue` TPS·p95/p99, `coupon_rejected{code:SOLD_OUT}` | 발급 p99 < 500ms, 발급 정확히 500 |
| S4 | 쿠폰 중복 버스트 | DUP_USERS 200 × BURST 5 | `coupon_dup_violation`, DB 사용자 중복 | 위반 0 |
| S5 | 입장 배치 크기 비교 | S1 고정, `QUEUE_ADMISSION_BATCH_SIZE` 50 / 100 / 500 | `queue_wait_ms` vs `entry_apply` p99 트레이드오프 | 결과 기록 (목표 없음) |
| S6 | Redis 재시작 내구성 | S1 직후 `docker compose restart redis` | 재시작 후 remaining·applicants 유지 | 재검증 PASS (AOF 효과 확인) |

> 목표 수치는 **로컬 단일 머신** 기준의 출발점이다. 처음 측정 후 리포트에 근거와 함께 조정한다.
> 추첨 검증: S1·S2 는 `ENTRY_WINDOW_SEC` 후 마감되므로, 마감 + 추첨 주기(5초) + 여유 후 `verify-drop.sh <id> --after-draw` 를 실행한다.

### 3.7 리포트 템플릿 — `docs/report/load-test-report.md`
```markdown
# 부하 테스트 리포트

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|

## 1. 측정 환경
| 항목 | 값 |
|------|----|
| 머신 | (CPU, 코어 수, 메모리, OS) |
| Docker 리소스 | (Docker Desktop 할당 CPU/메모리) |
| 컨테이너 메모리 제한 | (서비스별) |
| 버전 | Java 21, Spring Boot 4.1.2, PostgreSQL 17, Redis 7.4, Kafka 4.0, k6 (버전) |
| 주의 | k6 와 서비스가 같은 머신에서 CPU 를 나눠 씀 |

## 2. 시나리오별 결과
### S1. 드로우 오픈 스파이크
- 파라미터: USERS=, CAPACITY=, WINNERS=, BATCH=, INTERVAL=, TOKEN_TTL=
- dropId: (k6 출력)

| 지표 | queue_enter | queue_status | entry_apply |
|------|-------------|--------------|-------------|
| 요청 수 | | | |
| TPS (최대 / 평균) | | | |
| p50 / p95 / p99 (ms) | | | |
| 5xx 비율 | | | |

| 결과 카운트 | 값 |
|-------------|----|
| entry_ok (202) | |
| entry_rejected (code 별) | |
| entry_retryable_503 | |
| queue_sold_out / queue_lost / queue_timeout | |
| queue_wait_ms p50 / p95 | |
| entry-service lag 최대 / 해소 시간 | |

**정합성 검증** (`verify-drop.sh` 출력 붙여넣기)

### S2 ~ S6
(같은 형식)

## 3. 병목 분석
- 관찰: (어느 구간에서 지연이 늘었는지 — Gateway / Redis / Kafka ack / DB 저장)
- 근거: (메트릭, 서비스 측 http.server.requests 와 k6 지연 비교, hikaricp pending, lag 그래프)
- 원인 가설과 확인 방법

## 4. 개선 전후 비교
| 변경 | 지표 | 전 | 후 | 해석 |
|------|------|----|----|------|

## 5. 결론
- 정원·수량 초과 0, 중복 0 여부
- 설계 결정(D1~D7)이 실제로 효과가 있었는지
- 한계와 다음 개선 과제
```

### 3.8 선택 과제 — DB 락 방식과 비교 실험 (방법만)
**목적**: Redis Lua 판정을 고른 이유([D1](../../design/04-non-functional/concurrency-consistency.md))를 수치로 보여준다.

**방법**
1. entry-service 에 `EntryGate` 의 두 번째 구현체 `DbEntryGate` 를 `@Profile("gate-db")` 로 추가한다. 4계층 + DIP 구조라 **application·presentation 코드는 그대로 두고 infrastructure 구현체만 바꿔 끼울 수 있다** (이 실험 자체가 DIP 효과의 시연이 된다).
   - 판정: 하나의 DB 트랜잭션에서 `UPDATE drop_capacity SET remaining = remaining - 1 WHERE drop_id = ? AND remaining > 0` (조건부 UPDATE) → 영향 행 1 이면 `INSERT entries` (UNIQUE 로 중복 방지). 변형으로 `SELECT ... FOR UPDATE` 방식도 측정.
   - 정원 행(`drop_capacity`)은 실험용 테이블 (구현 결정, 기본 프로필에서는 사용하지 않음).
   - 토큰 확인은 동일하게 Redis 를 쓰고, Kafka 발행은 저장 이후로 둔다 (비교 대상을 "판정 방식"으로 한정).
2. 같은 S1 파라미터로 `SPRING_PROFILES_ACTIVE=gate-db` 와 기본을 번갈아 측정한다.
3. 비교 지표: `entry_apply` TPS·p95/p99, 5xx, PostgreSQL 락 대기(`pg_stat_activity` 의 `wait_event_type = 'Lock'` 표본), `hikaricp.connections.pending`, 정합성(둘 다 초과 0 이어야 함).
4. 리포트 4장에 "정합성은 같고 처리량·지연이 어떻게 다른지"를 기록한다.

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 (시나리오) | 시나리오 (Given / When / Then) | 관련 |
|----|-----------|-------------------|-------------------------------|------|
| T8-01 | 인프라 | 컨테이너 기동 | Given `bootJar` 완료 / When `--profile app up -d --build` / Then 서비스 7개 healthy | — |
| T8-02 | 인프라 | 내부 포트 비노출 | When 호스트에서 `curl localhost:8081~8086` / Then 연결 거부, `localhost:8080` 만 응답 | architecture 6장 |
| T8-03 | 인프라 | 헤더 위조 차단 (컨테이너 환경) | When `X-User-Id: 1` 만 붙여 gateway 로 응모 / Then 401 | REQ-USER-03 |
| T8-04 | 준비 | `prepare-users.js` 재실행 | Given 이미 생성된 사용자 / When 재실행 / Then 전부 201 또는 409, `unexpected` 0 | — |
| T8-05 | 부하 S1 | `drop-open-spike.js` 기본 | Given USERS 3,000 / M 1,000 / When 실행 / Then k6 임계값 통과, `verify-drop.sh` 전부 PASS, `entry_ok` == DB 응모 수 | NFR-01~03 |
| T8-06 | 부하 S1 (추첨) | `verify-drop.sh --after-draw` | Given S1 드로우 마감 + 추첨 주기 경과 / Then DRAWN, 당첨 수 == min(K, 응모자), draw_results 1건 | REQ-ENTRY-05 |
| T8-07 | 부하 S2 | 정원 조기 소진 | Given USERS 5,000 / M 100 / Then `entry_ok` == 100, DB 100, 소진 이후 `queue.admitted` 증가 멈춤 | REQ-ENTRY-02, REQ-QUEUE-04 |
| T8-08 | 부하 S3 | `coupon-spike.js` spike | Given USERS 3,000 / 수량 500 / Then `coupon_ok` == 500, `verify-coupon.sh` PASS | REQ-COUPON-02, NFR-01 |
| T8-09 | 부하 S4 | `coupon-spike.js` dup_burst | Given 200명 × 동시 5회 / Then `coupon_dup_violation` 0, DB 사용자 중복 0 | REQ-COUPON-03, NFR-02 |
| T8-10 | 부하 S5 | 배치 크기 50/100/500 | Then 결과표 작성 (`queue_wait_ms` vs `entry_apply` p99) | REQ-QUEUE-04 |
| T8-11 | 내구성 S6 | Redis 재시작 | Given S1 완료 / When `docker compose restart redis` / Then `verify-drop.sh` 재실행 PASS (remaining·applicants 유지) | NFR-01, infrastructure 5장 |
| T8-12 | 관측 | lag 해소 시간 | Given S1 종료 / When `kafka-lag.sh entry-service` 반복 / Then LAG 0 도달 시간 기록 | NFR-03 |
| T8-13 | 관측 | 보상 발생 여부 | Then `entry.apply.compensated` == 0 (발생 시 원인 분석을 리포트에 기록) | D2 |
| T8-14 | 문서 | 리포트 | 측정 환경·파라미터·수치·정합성 출력·병목 분석이 템플릿 항목대로 채워짐 | — |
| T8-15 | 선택 | DB 락 비교 | `gate-db` 프로필로 S1 → 정합성 PASS, 지표 비교표 작성 | D1 |

---

## 5. 커밋 계획

| 순서 | 커밋 메시지 | 포함 |
|------|-------------|------|
| 1 | `chore: 서비스 컨테이너 이미지 및 compose app 프로필 추가`<br>- 공용 Dockerfile (bootJar 기반)<br>- app 프로필: 서비스 7개, gateway 만 외부 노출<br>- plain jar 비활성화 | `docker/`, `.dockerignore`, `docker-compose.yml`, 루트 `build.gradle` |
| 2 | `chore: Actuator 메트릭 노출 및 튜닝 파라미터 환경변수화`<br>- health·metrics·prometheus 엔드포인트, 지연 히스토그램<br>- 입장 배치·토큰 TTL·발행 타임아웃 등 환경변수로 조정 | 각 서비스 `application.yml`, Prometheus 의존성 |
| 3 | `feat: 판정 결과·보상·입장 커스텀 메트릭 추가`<br>- entry/coupon 판정 결과 카운터 (presentation)<br>- 보상·입장 카운터 (infrastructure) | entry, coupon, queue 서비스 |
| 4 | `test: k6 부하 테스트 스크립트 추가`<br>- 사용자 사전 생성, 드로우 오픈 스파이크, 쿠폰 스파이크·중복 버스트<br>- compose loadtest 프로필 | `load-test/`, compose `k6` |
| 5 | `test: 정합성 검증 및 컨슈머 lag 조회 스크립트 추가`<br>- 정원·수량 초과, 중복, Redis↔DB 일치, 추첨 결과 검증 | `scripts/` |
| 6 | `chore: Prometheus·Grafana 모니터링 프로필 추가` (선택) | compose `monitoring`, `infra/monitoring/` |
| 7 | `docs: 부하 테스트 리포트 작성`<br>- 시나리오 S1~S6 결과, 병목 분석, 개선 전후 비교 | `docs/report/` |
| 8 | `perf: …` (측정 결과에 따른 개선 — 항목별 별도 커밋) | 해당 서비스 |
| 9 | `test: DB 락 방식 판정 비교 실험` (선택) | entry-service `DbEntryGate`, 리포트 |

---

## 6. 결정 필요 · 리스크

### 결정 필요
| 항목 | 기본안 | 비고 |
|------|--------|------|
| 이미지 빌드 방식 | **호스트 bootJar + 공용 Dockerfile** (구현 결정) | 재현성이 중요해지면 멀티 스테이지로 전환 |
| 초기 목표 수치 | 3.6 표 (구현 결정) | 첫 측정 후 근거와 함께 조정 |
| 사용자 수 기본값 | USERS 3,000 (구현 결정) | k6 VU 메모리에 따라 조정 (아래 리스크) |
| 모니터링 스택 | Actuator + CLI 우선, Prometheus·Grafana 는 선택 | 병목 분석이 막히면 도입 |

### 설계 수정 필요
- **관리자 계정 생성 방법이 설계에 없다.** 드로우·쿠폰 이벤트 등록은 ADMIN 권한이 필요한데, 회원가입은 USER 만 만든다. 기본안: user-service 기동 시 `ADMIN_EMAIL`/`ADMIN_PASSWORD` 환경변수가 있으면 관리자 계정을 없을 때만 생성 (1단계에서 미반영 시 8단계에서 추가). [requirements.md](../../design/01-requirements/requirements.md) 액터(관리자)·[architecture.md](../../design/02-architecture/architecture.md) 6장에 생성 방식 명시 필요.
- **튜닝 프로퍼티 이름 통일**: 3.2 표의 프로퍼티 이름은 가정이다. 3·4·5·6단계 구현 상세와 다르면 그쪽 이름을 따르고, [infrastructure.md](../../design/03-infrastructure/infrastructure.md) 4장 환경변수 표에 추가해야 한다.
- **[infrastructure.md](../../design/03-infrastructure/infrastructure.md) 3장**: app / loadtest / monitoring 프로필과 "서비스 컨테이너 환경에서는 `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`" 를 반영해야 한다.

### 리스크
- **k6 와 서비스가 같은 머신**: 부하 발생기가 CPU 를 나눠 써서 서비스 처리량이 낮게 측정될 수 있다. 리포트에 명시하고, 가능하면 k6 를 다른 머신에서 실행한다.
- **k6 VU 메모리**: VU 하나당 수 MB 가 들어 3,000~5,000 VU 는 수 GB 가 필요할 수 있다. 첫 실행에서 `docker stats` 로 확인하고 USERS 를 조정한다.
- **로그인 BCrypt 부하**: 모든 VU 가 시작 시 로그인 → user-service CPU 급증. `OPEN_DELAY_SEC` 안에 로그인이 끝나는지 확인 (끝나지 않으면 지연 늘리기).
- **JWT 만료**: 기본 만료가 짧으면 긴 대기열 폴링 중 401 이 난다 → 부하 테스트 시 `JWT_EXPIRATION=2h`.
- **입장 처리량 상한**: 배치 100 · 1초면 초당 100명만 응모 단계에 도달한다. 응모 API 의 한계를 보려면 배치를 키운 S5 결과를 함께 봐야 한다 (대기열이 의도대로 응모 부하를 막고 있다는 증거이기도 함).
- **k6 결과 디렉터리 권한**: `grafana/k6` 이미지는 비루트 사용자로 실행된다. `/results` 마운트에 쓰기 실패하면 디렉터리 권한을 열거나 `user:` 를 지정한다.
- **`handleSummary` 의 `setup_data`, `jslib` 원격 import**: k6 버전에 따라 동작이 다를 수 있다 (구현 시 확인). 네트워크가 막힌 환경이면 `textSummary` 를 로컬 파일로 내려받는다.
- **시계**: 오픈 시각 판정은 서비스 시각, 대기는 k6 시각. 같은 머신이라 차이는 작지만, 이르게 도착한 요청을 위해 `NOT_OPEN` 시 1회 재시도를 넣었다.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 / 작업 | 테스트 ID |
|-----------------------|-----------|
| [ ] 서비스 컨테이너화 (Dockerfile, app 프로필) | T8-01 |
| [ ] 내부 포트 비노출 | T8-02, T8-03 |
| [ ] 관측: Actuator 메트릭 (+ 선택 Prometheus·Grafana) | T8-12, T8-13 |
| [ ] k6 드로우 오픈 스파이크 | T8-05, T8-07 |
| [ ] k6 쿠폰 스파이크 | T8-08, T8-09 |
| [ ] 결과 검증 스크립트: DB 응모 수 ≤ M, 쿠폰 발급 수 ≤ 수량, 중복 0 | T8-05~T8-09, T8-11 |
| [ ] **부하 테스트 중 정원·수량 초과 0, 중복 0** | T8-05, T8-07, T8-08, T8-09 |
| [ ] 추첨 결과 정합성 | T8-06 |
| [ ] 리포트에 측정 조건·수치·해석 기록 (`docs/report/`) | T8-10, T8-14 |
| [ ] (선택) DB 락 방식 비교 | T8-15 |
| [ ] 공통: `./gradlew build` 통과 (ArchUnit 포함) | — |
| [ ] 공통: "설계 수정 필요" 3건 반영 (관리자 계정, 프로퍼티 이름, 인프라 프로필) | — |
