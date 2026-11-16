# 응모 설계 문서

## 변경 이력
| 버전 | 날짜 | 변경 내용 | 관련 요구사항 |
|------|------|-----------|----------------|
| v1.0 | 2026-11-16 | 최초 작성 | REQ-ENTRY-01 ~ 04, 06 |
| v1.1 | 2026-11-16 | 클래스 다이어그램을 4계층 구조로 이관 (class-diagram.md 참고) | layered-architecture.md |
| v1.2 | 2026-11-16 | entrySeq 발급을 잔여 정원 계산에서 별도 INCR 카운터로 변경 (보상 후 번호 재사용으로 인한 응모 누락 방지) | NFR-03 |
| v1.3 | 2026-11-16 | 발행 재시도를 producer 내부 멱등 재시도로 변경, 저장 전 Redis 확인 확정, 저장 방식(ON CONFLICT)·DLT, 조회 보완 범위(단건만), 에러 코드 추가, meta 없음 → CLOSED | phase-4 구현 상세 6장 |
| v1.4 | 2026-11-16 | 에러 코드 표를 명명 규칙 `{SERVICE}_{REASON}` 에 맞춤 (`ENTRY_*`, `COMMON_MISSING_USER_HEADER`). 판정 결과 `ApplyOutcome` 값 이름은 그대로 | layered-architecture.md 4.5 v1.3 |

## 1. 요구사항 요약 (근거)
- REQ-ENTRY-01: 입장 토큰으로 응모
- REQ-ENTRY-02: 선착순 M명, 정원 차면 마감
- REQ-ENTRY-03: 1인 1드로우 1응모
- REQ-ENTRY-04: 취소 불가
- REQ-ENTRY-06: 내 응모 내역·결과 조회
- 비기능 결정 D1, D2, D3

## 2. 쉬운 설명 (한눈에)
> 입장 팔찌를 찬 사람이 응모 버튼을 누르면, 메모장(Redis)이 한 번에 "팔찌 있나? 이미 응모했나? 자리 남았나?"를 확인하고 자리를 하나 줘요.
> 그 다음 장부 기록용 편지를 우체통(Kafka)에 넣고, 넣었다는 확인을 받아야 "응모 완료"라고 답해요.
> 편지를 못 넣으면 줬던 자리를 다시 회수하고 "다시 시도해 주세요"라고 답해요.

## 3. 상세 설명

### 응모 Lua 스크립트 (원자 판정)
입력: `KEYS = [entry meta, remaining, applicants, seq, queue token]`, `ARGV = [userId, nowEpochMs]`

| 순서 | 검사 | 실패 시 결과 |
|------|------|--------------|
| 1 | meta 존재, meta.state == OPEN 그리고 openAt ≤ now < closeAt | `CLOSED` (meta 없음 = 드로우 정보 미수신도 `CLOSED`) |
| 2 | `EXISTS token` | `NO_TOKEN` |
| 3 | `SISMEMBER applicants userId` == 0 | `DUPLICATE` |
| 4 | `GET remaining` > 0 | `SOLD_OUT` |
| 5 | `DECR remaining`, `SADD applicants userId` | — |
| 6 | `entrySeq = INCR entry:{dropId}:seq` 반환 | `OK(entrySeq)` |

- 시간 비교에 `nowEpochMs` 를 인자로 받는 이유: Lua 안에서 `TIME` 을 쓰면 복제 시 비결정적 — 애플리케이션 시각을 넘기고 판정은 한 곳(Redis)에서만.
- `entrySeq` 는 **잔여 정원과 무관한 별도 카운터**로 발급하는 드로우 내 고유 번호 → 추첨 시 결정적 정렬 기준.
  - 카운터는 **증가만 하고 보상 시에도 되돌리지 않는다.** 보상된 응모의 번호는 빈 번호로 남고, 최댓값이 M 보다 커질 수 있다.
  - 이유: 잔여 정원에서 번호를 계산(`M - remaining`)하면 보상(`INCR remaining`) 후 같은 번호가 다른 사용자에게 재발급되어, 유령 이벤트와 정상 이벤트가 `UNIQUE(drop_id, entry_seq)` 에서 충돌해 **정상 응모가 누락**될 수 있다 ([sequence-diagram.md](../08-sequence-diagram/sequence-diagram.md) 5.2).

### 이벤트 발행 + 보상 (D2)
- `kafkaTemplate.send(entry.created).get(3.5s)` 로 동기 확인
- producer: `acks=all`, `enable.idempotence=true`, `retries=MAX`, `request.timeout.ms=1000`, `delivery.timeout.ms=3000`
- **애플리케이션 재시도 없음** — 같은 내용을 다시 send 하면 별개 레코드가 되어 유령 이벤트를 늘린다 ([D2](../04-non-functional/concurrency-consistency.md))
- 실패 → 보상 Lua: `if SREM applicants userId == 1 then INCR remaining end` (`seq` 는 되돌리지 않음)
- 응답: `503 TEMPORARILY_FAILED` (재시도 가능)

### 저장 컨슈머 (entry-service 자기 토픽 소비)
1. `entry.created` 소비
2. **저장 전 Redis 확인 (확정)**: `SISMEMBER applicants userId` 가 0 이면 보상된 응모(유령 이벤트)로 보고 저장하지 않는다
3. `INSERT entries (...) ON CONFLICT (drop_id, user_id) DO NOTHING` — 이미 저장된 응모면 0건 반영으로 끝난다 (멱등)
   - 예외(`DataIntegrityViolationException`)를 잡아 무시하는 방식은 쓰지 않는다: 트랜잭션이 rollback-only 가 되고 세션을 다시 쓸 수 없다
   - 충돌 대상을 `(drop_id, user_id)` 로만 지정한다. `entry_seq` 충돌은 정상이라면 일어나지 않는 버그이므로 예외로 드러낸다
4. DB 장애 등 처리 실패 → 1초 간격 3회 재시도 후 `entry.created.DLT`
- 수동 커밋(`ack-mode=record`). 배치 리스너 + bulk insert 는 부하 측정 후 검토

### API
| Method | Path | 설명 |
|--------|------|------|
| POST | `/api/entries/drops/{dropId}` | 응모 → `202 Accepted {entrySeq}` |
| GET | `/api/entries/me` | 내 응모 목록 (상태: APPLIED/WON/LOST) |
| GET | `/api/entries/drops/{dropId}/me` | 특정 드로우 내 응모·결과 |

- 응답이 `202` 인 이유: 판정은 확정이지만 DB 저장은 비동기. 직후 조회 시 아직 없을 수 있으므로, **단건 조회(`/drops/{dropId}/me`)만** DB 에 없으면 Redis `SISMEMBER` 로 보완해 `APPLIED` 로 응답한다. 목록(`/me`)은 DB 만 조회한다 (드로우마다 Redis 를 확인하는 비용을 피함).

### 에러 코드
| 코드 | HTTP | 발생 |
|------|------|------|
| `ENTRY_CLOSED` | 409 | 오픈 전·마감 후·드로우 정보 미수신 |
| `ENTRY_NO_TOKEN` | 409 | 입장 토큰 없음·만료 |
| `ENTRY_DUPLICATE` | 409 | 이미 응모함 |
| `ENTRY_SOLD_OUT` | 409 | 정원 소진 |
| `ENTRY_TEMPORARILY_FAILED` | 503 | 이벤트 발행 실패 → 보상 완료, 재시도 가능 |
| `ENTRY_NOT_FOUND` | 404 | 단건 조회 시 DB·Redis 모두 없음 |
| `COMMON_MISSING_USER_HEADER` | 400 | `X-User-Id` 누락 (Gateway 우회 등) |

- `drop.created` 의 데이터가 도메인 불변식을 어기면(예: M < K) application 이 `InvalidDropSnapshotException` 으로 바꿔 던지고, 리스너 재시도 후 `drop.created.DLT` 로 간다.

## 4. 설계 결정
| 결정점 | 선택 | 이유 / 포기한 것 |
|--------|------|------------------|
| 판정 위치 | Redis Lua 하나로 4가지 검사 + 차감 | 원자성, DB 락 없음 / Redis 에 판정 상태 의존 |
| 토큰 검증 | 같은 Lua 안에서 EXISTS | 검증과 차감 사이 토큰 만료 경합 없음 / queue 키 읽기 의존 |
| 성공 기준 | Kafka 수신 확인까지 | "성공 = 기록 보장" 불변식 / 응답 지연에 Kafka 왕복 포함 |
| 토큰 소모 | 응모 후 삭제하지 않음 | 보상 시 토큰 복구 불필요 / 토큰이 TTL 까지 남음 (재응모는 중복 검사가 막음) |

## 5. 다이어그램

### 5.1 시퀀스 다이어그램 (단순)
```mermaid
sequenceDiagram
    actor U as 사용자
    participant ES as entry-service
    participant R as Redis
    participant K as Kafka
    U->>ES: 응모
    ES->>R: Lua 판정
    R-->>ES: OK(entrySeq)
    ES->>K: entry.created
    ES-->>U: 202 응모 완료
    K->>ES: (컨슈머) DB 저장
```

### 5.2 시퀀스 다이어그램 (상세)
**왜**: 응답 경로에서 무엇이 동기이고 무엇이 비동기인지, 실패 시 어디까지 되돌리는지(트랜잭션 경계) 확인.

```mermaid
sequenceDiagram
    actor U as 사용자
    participant GW as Gateway
    participant ES as entry-service
    participant R as Redis
    participant K as Kafka
    participant C as EntryConsumer
    participant DB as entry_db
    participant RS as ranking-service

    U->>GW: POST /api/entries/drops/{id}
    GW->>ES: X-User-Id
    ES->>R: EVALSHA apply.lua (userId, now)
    alt CLOSED / NO_TOKEN / DUPLICATE / SOLD_OUT
        R-->>ES: 실패 코드
        ES-->>U: 409 + 사유
    else OK(entrySeq)
        R-->>ES: entrySeq
        ES->>K: send(entry.created).get(3.5s) (producer 내부 재시도만)
        alt 발행 성공
            K-->>ES: ack
            ES-->>U: 202 {entrySeq}
        else 최종 실패
            ES->>R: EVALSHA compensate.lua
            ES-->>U: 503 다시 시도
        end
    end

    par 저장
        K->>C: entry.created
        C->>R: SISMEMBER applicants (없으면 스킵)
        C->>DB: INSERT ... ON CONFLICT DO NOTHING
    and 랭킹
        K->>RS: entry.created
    end
```

**읽는 법**
- 동기 경계는 **Redis Lua + Kafka 발행**까지다. DB 저장과 랭킹 집계는 응답 이후 일이다.
- 실패 분기는 두 종류다: **비즈니스 실패(409)** 는 Redis 에서 아무것도 바뀌지 않았고, **기술 실패(503)** 는 보상으로 되돌린 상태다.
- 저장 컨슈머와 랭킹 컨슈머는 서로 독립이다. 랭킹이 늦어도 응모 저장에는 영향이 없다.
- 저장 컨슈머가 Redis 를 먼저 확인하는 이유: 503 으로 보상된 응모의 이벤트가 뒤늦게 Kafka 에 기록되어 있을 수 있기 때문이다 (유령 이벤트).

### 5.3 클래스 다이어그램
4계층 구조의 상세 다이어그램은 [class-diagram.md](../07-class-diagram/class-diagram.md) 1.1 (응모) 참고.

**이 기능에서 봐야 할 포인트**
- 응답 경로 `EntryController → EntryApplyService → EntryGate(Redis) / EntryEventPublisher(Kafka)` 에 **DB(EntryRepository) 가 없다**.
- DB 쓰기는 `EntryCreatedListener → EntrySaveService → EntryRepository` 한 경로뿐이다 (쓰기 경로 단일화).
- 판정 결과 `ApplyOutcome` 은 domain enum 이지만, presentation 은 이를 직접 보지 않고 application 예외의 에러 코드로 409 사유를 받는다.

### 5.4 ERD
[erd.md](../06-erd/erd.md) 의 entry_db (`entries`) 참고.

## 6. 잠재 리스크
- **Kafka 발행 타임아웃 후 실제로는 성공**: 보상으로 Redis 에서 빠졌는데 Kafka 에는 기록됨 → 저장 컨슈머의 저장 전 `SISMEMBER` 확인으로 걸러낸다 (확정). `delivery.timeout.ms` 로 send 결과를 3초 안에 확정시켜 이런 경우 자체를 줄인다.
- **응답 지연 상한**: Kafka 가 느리면 응모 응답이 최대 약 3.5초까지 늘 수 있다.
- **응답 지연**: 피크 시 Kafka 왕복이 응답 시간을 좌우 → 부하 테스트로 측정 필요 (`linger.ms`, `batch.size` 튜닝 지점).
- **Redis 재시작**: AOF 없으면 remaining/applicants 유실 → 오버 응모 가능. **AOF everysec 필수**, 그래도 최대 1초 유실 가능성은 남음.
- **긴 Lua 스크립트**: 현재 O(1) 명령만 사용해 문제 없음. 스크립트에 반복문이 들어가면 Redis 전체가 멈추므로 금지.
