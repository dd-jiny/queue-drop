# 시퀀스 다이어그램

## 변경 이력
| 버전 | 날짜 | 변경 내용 | 관련 요구사항 |
|------|------|-----------|----------------|
| v1.0 | 2026-11-16 | 최초 작성 (인증, 전체 흐름, 쿠폰, 랭킹, 장애 시나리오) | 전체 |
| v1.1 | 2026-11-16 | 5.2 응모 번호 재사용 문제 해결 반영 (별도 seq 카운터) 및 해결 후 시퀀스 추가 | NFR-03 |

## 쉬운 설명
> 기능 문서(`05-features/`)의 시퀀스가 "창구 하나에서 무슨 일이 일어나는지"라면,
> 이 문서는 "사용자 한 명이 처음부터 끝까지 어떤 창구들을 거쳐 가는지"와 "중간에 무언가 고장 나면 어떻게 되는지"를 보여줘요.

## 이 문서의 범위
| 구분 | 이 문서 | 기능 문서 (상세) |
|------|---------|------------------|
| 관점 | 서비스 간 전체 흐름, 장애 시나리오 | 기능 하나의 분기·예외·Redis 명령 수준 |
| 목록 | 1. 인증 / 2. 드로우 전체 흐름 / 3. 선착순 쿠폰 / 4. 랭킹 / 5. 장애 시나리오 | [drop-register](../05-features/drop-register.md) · [queue-enter](../05-features/queue-enter.md) · [entry-apply](../05-features/entry-apply.md) · [draw](../05-features/draw.md) · [coupon-issue](../05-features/coupon-issue.md) · [ranking](../05-features/ranking.md) |

**표기 규칙**
- 실선 화살표(`->>`): 동기 호출 / 점선(`-->>`): 응답
- `Kafka` 참여자를 거치는 화살표: 비동기 이벤트 (응답을 기다리지 않음)
- `rect` 박스: 하나의 DB 트랜잭션 또는 하나의 원자 연산(Lua)

---

## 1. 회원가입 · 로그인 · 인증 요청 처리

**왜**: 모든 기능의 전제. Gateway 가 어디서 신원을 확정하고, 내부 서비스는 무엇을 믿는지(신뢰 경계) 확인한다.
**근거**: REQ-USER-01 ~ 03, [architecture.md](../02-architecture/architecture.md) 6장

```mermaid
sequenceDiagram
    actor U as 사용자
    participant GW as Gateway
    participant US as user-service
    participant DB as user_db
    participant K as Kafka
    participant RS as ranking-service
    participant X as 내부 서비스(예: entry)

    Note over U,US: 회원가입 (인증 제외 경로)
    U->>GW: POST /api/users/signup
    GW->>US: 그대로 전달
    US->>DB: INSERT users (email UK, password_hash)
    US-->>U: 201
    US->>K: user.registered (userId, nickname)
    K->>RS: 닉네임 저장 (랭킹 표시용)

    Note over U,US: 로그인 (인증 제외 경로)
    U->>GW: POST /api/users/login
    GW->>US: 전달
    US->>DB: SELECT by email
    US->>US: 비밀번호 검증, JWT 서명 (sub=userId, role)
    US-->>U: 200 {accessToken}

    Note over U,X: 인증이 필요한 요청
    U->>GW: Authorization: Bearer JWT (+ 위조 X-User-Id)
    GW->>GW: 클라이언트 X-User-* 헤더 제거
    GW->>GW: JWT 서명·만료 검증
    alt 검증 실패
        GW-->>U: 401
    else 성공
        GW->>X: X-User-Id, X-User-Role 주입 후 라우팅
        X-->>U: 응답
    end
```

**읽는 법**
- **신뢰 경계는 Gateway 하나**다. 내부 서비스는 JWT 를 다시 검증하지 않고 `X-User-Id` 헤더를 믿는다 → 내부 포트가 외부에 노출되면 이 전제가 깨진다.
- 헤더 제거가 검증보다 **먼저**다. 순서가 바뀌면 검증 실패 경로 등에서 위조 헤더가 남을 여지가 생긴다.
- 회원가입 응답과 `user.registered` 발행은 분리되어 있어, 랭킹 닉네임 반영이 늦어도 가입에는 영향이 없다.

---

## 2. 드로우 전체 흐름 (End-to-End, 정상 경로)

**왜**: 드로우 하나가 등록되어 추첨될 때까지 6개 서비스가 어떤 순서로 관여하는지 한 장으로 확인한다. 각 단계의 상세 분기는 기능 문서를 본다.
**근거**: REQ-DROP-02, REQ-QUEUE-*, REQ-ENTRY-*, REQ-RANK-03

### 2.1 단순 버전
```mermaid
sequenceDiagram
    actor A as 관리자
    actor U as 사용자
    participant PS as product
    participant QS as queue
    participant ES as entry
    participant RS as ranking

    A->>PS: ① 드로우 등록
    PS-)QS: drop.created
    PS-)ES: drop.created
    U->>QS: ② 대기열 진입
    QS-->>U: 입장 토큰 발급
    U->>ES: ③ 응모
    ES-->>U: 응모 완료
    ES-)RS: entry.created → 랭킹 +1
    Note over ES: ④ 마감 시각
    ES->>ES: 추첨
    ES-)PS: draw.completed
    U->>ES: ⑤ 결과 조회
    ES-->>U: 당첨 / 낙첨
```

### 2.2 상세 버전
```mermaid
sequenceDiagram
    actor A as 관리자
    actor U as 사용자
    participant GW as Gateway
    participant PS as product-service
    participant QS as queue-service
    participant ES as entry-service
    participant RS as ranking-service
    participant R as Redis
    participant K as Kafka
    participant EDB as entry_db

    rect rgb(245, 245, 245)
    Note over A,EDB: ① 드로우 등록 · 전파 (오픈 전)
    A->>GW: POST /api/products/{id}/drops
    GW->>PS: X-User-Role=ADMIN
    PS->>PS: INSERT drops (commit)
    PS-->>A: 201
    PS->>K: drop.created
    K->>ES: drop_snapshots 저장, Redis meta·remaining=M (SET NX)
    K->>QS: Redis queue meta
    K->>RS: 드로우 제목 저장
    end

    rect rgb(235, 245, 255)
    Note over U,R: ② 대기열 (openAt 이후)
    U->>GW: POST /api/queue/drops/{id}
    GW->>QS: X-User-Id
    QS->>R: INCR seq, ZADD NX waiting
    QS-->>U: WAITING (앞에 N명)
    loop 1초마다 (입장 스케줄러)
        QS->>R: Lua: ZPOPMIN N + SET token EX 300
    end
    U->>QS: GET 상태 (폴링)
    QS-->>U: ADMITTED
    end

    rect rgb(235, 255, 235)
    Note over U,EDB: ③ 응모
    U->>GW: POST /api/entries/drops/{id}
    GW->>ES: X-User-Id
    ES->>R: Lua: 마감·토큰·중복·잔여 검사 + DECR + SADD
    R-->>ES: OK(entrySeq)
    ES->>K: entry.created (ack 대기)
    ES-->>U: 202 {entrySeq}
    par
        K->>ES: EntryCreatedConsumer → INSERT entries
        ES->>EDB: INSERT (UNIQUE 위반 무시)
    and
        K->>RS: Lua: dedupe + ZINCRBY ×3
    end
    end

    rect rgb(255, 245, 230)
    Note over ES,EDB: ④ 마감 · 추첨 (closeAt 이후)
    ES->>EDB: UPDATE status OPEN→CLOSING (조건부)
    ES->>R: meta state=CLOSED
    ES->>R: SCARD applicants
    ES->>EDB: COUNT(entries)
    Note over ES: 일치할 때까지 주기 재확인
    ES->>EDB: [TX] 셔플(seed) → WON/LOST, draw_results, DRAWN
    ES->>K: draw.completed
    K->>PS: drops.drawn = true
    end

    rect rgb(250, 240, 250)
    Note over U,ES: ⑤ 결과 조회
    U->>GW: GET /api/entries/drops/{id}/me
    GW->>ES: X-User-Id
    ES->>EDB: SELECT entries
    ES-->>U: WON / LOST
    end
```

**읽는 법**
- 사용자가 **응답을 기다리는 동기 구간**은 ②의 진입·폴링, ③의 Lua + Kafka ack, ⑤의 조회뿐이다. DB 쓰기(③ 저장, ④ 추첨)와 랭킹은 모두 사용자 응답 밖에 있다.
- ③에서 Redis 를 읽는 건 entry 지만, **토큰 키를 쓴 건 ②의 queue** 다 — 두 서비스가 만나는 유일한 지점이 Redis 키 하나라는 점이 그림에 드러난다.
- ④는 사용자 요청 없이 **스케줄러가 시작**한다. 사용자는 ⑤에서 폴링 또는 재방문으로 결과를 확인한다 (알림은 범위 밖).

---

## 3. 선착순 쿠폰 (등록 → 발급 → 조회)

**왜**: 대기열 없이 피크 요청이 그대로 Redis 에 도달하는 흐름. 응모와 같은 판정 패턴을 쓰되 무엇이 다른지 확인한다.
**근거**: REQ-COUPON-01 ~ 05, 상세는 [coupon-issue.md](../05-features/coupon-issue.md)

```mermaid
sequenceDiagram
    actor A as 관리자
    actor U as 사용자
    participant GW as Gateway
    participant CS as coupon-service
    participant R as Redis
    participant K as Kafka
    participant DB as coupon_db

    A->>GW: POST /api/coupons/events (수량, 시작 시각)
    GW->>CS: ADMIN
    CS->>DB: INSERT coupon_events (commit)
    CS->>R: SET remaining NX, meta
    CS-->>A: 201

    Note over U,CS: 발급 시작 시각 이후, 동시 요청 폭주
    U->>GW: POST /api/coupons/events/{id}/issue
    GW->>CS: X-User-Id
    rect rgb(235, 255, 235)
    CS->>R: Lua: 시작 시각·중복·잔여 검사 + DECR + SADD
    end
    alt NOT_STARTED / DUPLICATE / SOLD_OUT
        CS-->>U: 409
    else OK
        CS->>K: coupon.issued (ack 대기)
        CS-->>U: 202
        K->>CS: CouponIssuedConsumer
        CS->>DB: INSERT issued_coupons (UNIQUE 위반 무시)
    end

    U->>GW: GET /api/coupons/me
    GW->>CS: X-User-Id
    CS->>DB: SELECT issued_coupons
    CS-->>U: 쿠폰 목록
```

**읽는 법**
- 응모(2.2 ③)와 비교하면 **토큰 검사 대신 시작 시각 검사**가 들어가고, 나머지 구조(Lua → ack → 컨슈머 저장)는 같다.
- 쿠폰 이벤트 등록은 같은 서비스 안이라 Kafka 를 거치지 않고 Redis 를 직접 초기화한다 (드로우 등록과의 차이).

---

## 4. 랭킹 (집계 · 조회)

**왜**: 랭킹은 다른 서비스를 호출하지 않고 이벤트만으로 동작한다. 쓰기(집계)와 읽기(조회)가 어떻게 분리되는지 확인한다.
**근거**: REQ-RANK-01 ~ 03, 상세는 [ranking.md](../05-features/ranking.md)

```mermaid
sequenceDiagram
    participant K as Kafka
    participant RC as RankingConsumer
    participant R as Redis
    actor U as 사용자
    participant GW as Gateway
    participant RQ as RankingQueryService

    Note over K,R: 집계 (쓰기 경로)
    K->>RC: drop.created
    RC->>R: HSET rank:drop:info
    K->>RC: user.registered
    RC->>R: HSET rank:user:info
    K->>RC: entry.created (dropId, userId, appliedAt)
    rect rgb(235, 255, 235)
    RC->>R: Lua: SADD dedupe → 1 이면<br/>ZINCRBY popular / weekly:{주차} / all
    end

    Note over U,RQ: 조회 (읽기 경로, 인증 불필요)
    U->>GW: GET /api/rankings/users/weekly
    GW->>RQ: 전달
    RQ->>R: ZREVRANGE rank:user:weekly:{이번 주} 0 9 WITHSCORES
    RQ->>R: HMGET rank:user:info (닉네임)
    RQ-->>U: [{순위, 닉네임, 응모 수}]

    U->>GW: GET /api/rankings/users/me (JWT)
    GW->>RQ: X-User-Id
    RQ->>R: ZREVRANK + ZSCORE
    RQ-->>U: {순위, 점수}
```

**읽는 법**
- 주차 키는 **이벤트의 `appliedAt`** 으로 계산한다. 월요일 00:00 직전 응모가 지연 처리되어도 지난 주 랭킹에 들어간다.
- 같은 `entry.created` 가 두 번 와도 dedupe Set 이 막는다 — Kafka at-least-once 전제를 랭킹이 스스로 감당한다.

---

## 5. 장애 시나리오

**왜**: "Redis 판정 + Kafka 비동기 저장"(D1)을 고르면서 생긴 틈이 실제로 어떻게 메워지는지 확인한다. 정상 경로만 그리면 설계 결정의 비용이 보이지 않는다.
**근거**: [concurrency-consistency.md](../04-non-functional/concurrency-consistency.md) D2, D6

### 5.1 Kafka 발행 실패 → Redis 보상
```mermaid
sequenceDiagram
    actor U as 사용자
    participant ES as entry-service
    participant R as Redis
    participant K as Kafka

    U->>ES: 응모
    ES->>R: Lua 판정
    R-->>ES: OK(entrySeq) — 정원 1 차감됨
    ES-xK: entry.created (타임아웃)
    ES-xK: 재시도 1 (실패)
    ES-xK: 재시도 2 (실패)
    rect rgb(255, 235, 235)
    ES->>R: compensate.lua: SREM applicants == 1 이면 INCR remaining
    end
    ES-->>U: 503 다시 시도해 주세요
    Note over U,R: 정원·응모자 상태가 판정 전으로 복구 → 사용자는 재응모 가능 (토큰 TTL 내)
```
**읽는 법**: "성공 응답 = 이벤트 기록" 불변식을 지키기 위해 실패를 사용자에게 돌려준다. 토큰은 삭제하지 않았으므로 복구할 필요가 없다.

### 5.2 발행이 실제로는 성공했는데 타임아웃으로 판단한 경우
```mermaid
sequenceDiagram
    participant ES as entry-service
    participant R as Redis
    participant K as Kafka
    participant C as EntryCreatedConsumer
    participant DB as entry_db

    ES->>K: entry.created
    Note over K: 브로커는 기록함
    K--xES: ack 유실 → 타임아웃
    ES->>R: compensate (Redis 에서 응모자 제거)
    K->>C: entry.created 전달됨
    C->>R: SISMEMBER applicants userId
    alt Redis 에 없음 (보상됨)
        C->>C: 저장 스킵
    else 있음
        C->>DB: INSERT entries
    end
```
**읽는 법**: 컨슈머가 저장 전에 Redis 를 확인해 "보상된 응모"를 걸러낸다. 이 확인이 없으면 Redis 응모 수 < DB 응모 수가 되어 5.4 의 추첨이 영원히 대기한다. (README "확인 필요" 항목의 기본안)

> ✅ **응모 번호 재사용 문제 (v1.1 에서 해결)**
> 번호를 잔여 정원으로 계산(`M - remaining`)하면 보상(`INCR remaining`) 후 같은 번호가 다른 사용자에게 재발급된다.
> 보상된 사용자가 재응모하면 `SISMEMBER` 가 다시 true 가 되어 유령 이벤트가 저장되고, 같은 번호를 받은 다른 사용자의 INSERT 가
> `UNIQUE(drop_id, entry_seq)` 위반으로 "중복" 오인되어 **누락**될 수 있었다.
> → `entrySeq` 를 **되돌리지 않는 별도 카운터 `INCR entry:{dropId}:seq`** 로 발급하도록 변경. 번호가 겹치지 않으므로 유령 이벤트는
> 같은 사용자의 정상 이벤트와 `UNIQUE(drop_id, user_id)` 에서만 충돌하고, 다른 사용자의 응모에는 영향을 주지 않는다.

```mermaid
sequenceDiagram
    actor A as 사용자 A
    actor B as 사용자 B
    participant R as Redis
    participant K as Kafka
    participant C as Consumer
    participant DB as entry_db

    A->>R: 응모 → seq=1, remaining 99
    Note over K: (A, 1) 기록됐지만 ack 유실
    R->>R: 보상: SREM A, INCR remaining → 100 (seq 는 그대로 1)
    B->>R: 응모 → seq=2, remaining 99
    B->>K: (B, 2)
    A->>R: 재응모 → seq=3, remaining 98
    A->>K: (A, 3)
    K->>C: (A, 1) 유령 → SISMEMBER A = true
    C->>DB: INSERT (A, 1) 성공
    K->>C: (B, 2)
    C->>DB: INSERT (B, 2) 성공 ✅ 번호 충돌 없음
    K->>C: (A, 3)
    C->>DB: UNIQUE(drop_id, user_id) 위반 → 스킵
    Note over R,DB: Redis 응모자 {A, B} = 2, DB = 2 → 추첨 가능
```
**읽는 법**: A 는 유령 이벤트의 번호(1)로 저장되지만 응모 1건은 정확히 남는다. 번호는 추첨 정렬 기준일 뿐 당첨은 시드 셔플로 정해지므로 공정성에 영향이 없다.

### 5.3 컨슈머 재전달 (중복 이벤트)
```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as EntryCreatedConsumer
    participant DB as entry_db
    participant RC as RankingConsumer
    participant R as Redis

    K->>C: entry.created (1회차)
    C->>DB: INSERT 성공
    C--xK: 오프셋 커밋 전 재시작
    K->>C: entry.created (재전달)
    C->>DB: INSERT → UNIQUE(drop_id, user_id) 위반
    C->>C: 이미 저장됨으로 간주, 정상 처리

    K->>RC: entry.created (재전달)
    RC->>R: SADD dedupe → 0
    RC->>RC: 집계 스킵
```
**읽는 법**: 저장은 **DB 유니크 제약**으로, 랭킹은 **Redis dedupe Set** 으로 각자 멱등성을 확보한다. 두 컨슈머는 서로의 처리 여부를 모른다.

### 5.4 추첨 시 응모 수 불일치 (컨슈머 지연)
```mermaid
sequenceDiagram
    participant SCH as DrawScheduler
    participant R as Redis
    participant DB as entry_db
    participant K as Kafka
    participant C as EntryCreatedConsumer

    Note over SCH: closeAt 도달
    SCH->>DB: OPEN → CLOSING
    SCH->>R: state=CLOSED
    SCH->>R: SCARD = 1000
    SCH->>DB: COUNT = 987
    Note over SCH: 불일치 → 이번 주기 종료
    K->>C: 밀린 entry.created 13건
    C->>DB: INSERT ×13
    Note over SCH: 다음 주기 (5초 후)
    SCH->>R: SCARD = 1000
    SCH->>DB: COUNT = 1000
    rect rgb(235, 255, 235)
    SCH->>DB: [TX] 추첨 → DRAWN
    end
```
**읽는 법**: `CLOSING` 상태가 "닫혔지만 장부 정리 대기 중"을 표현한다. 불일치가 일정 시간 이상 계속되면 경고를 남기고 관리자 확정을 기다린다 ([draw.md](../05-features/draw.md) 6장).

---

## 잠재 리스크 (흐름 관점)
- **동기 구간의 Kafka 의존**: 2.2 ③과 3의 응답 시간에 Kafka ack 왕복이 들어간다. Kafka 가 느려지면 응모·쿠폰 응답이 같이 느려진다 → 부하 테스트에서 p99 확인 필요.
- **스케줄러 3종의 다중 인스턴스 동작**: 입장(queue), 추첨(entry)은 각각 분산 락 / 조건부 UPDATE 로 중복 실행을 막는다. 서비스를 여러 대로 늘리기 전에 두 장치가 실제로 동작하는지 테스트해야 한다.
- **사용자 폴링**: 대기열 상태(②)와 추첨 결과(⑤) 모두 폴링 기반이다. 대기자가 많으면 조회 요청이 응모 요청보다 많아질 수 있다.
