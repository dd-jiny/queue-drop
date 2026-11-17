# [비기능] 동시성·정합성 결정 기록

## 변경 이력
| 버전 | 날짜 | 변경 내용 | 관련 요구사항 |
|------|------|-----------|----------------|
| v1.0 | 2026-11-16 | 최초 작성 (Phase 1 합의) | NFR-01 ~ NFR-04 |
| v1.1 | 2026-11-16 | D2 남는 리스크에 '보상 후 응모 번호 재사용' 과 해결 방식(별도 seq 카운터) 추가 | NFR-03 |
| v1.2 | 2026-11-16 | D2 재시도를 producer 내부 멱등 재시도로 변경(애플리케이션 재시도 제거), 저장 전 Redis 확인 확정, D4 발행 방식 확정(어댑터 내 afterCommit), 저장 컨슈머 DLT 추가 | NFR-03, phase-4·6 구현 상세 6장 |
| v1.3 | 2026-11-17 | D2 유령 이벤트·Redis 타임아웃 대응 변경 (사용자 결정, 백로그 E-40·E-41): 요청 ID(eventId) 기록(`requests`), 저장 확정 표시(`persisted`, claim Lua)가 `SISMEMBER` 확인을 대체, 보상 Lua 가 3결과(COMPENSATED / ALREADY_PERSISTED / NOT_OURS) 반환 — 이미 저장 확정이면 되돌리지 않고 성공 응답. Redis 예외는 `GateUnavailableException` → 같은 eventId 로 보상 시도 후 503. 정합성 논증·남는 리스크 갱신. D3 에 entry 키 쓰기 소유 추가 | NFR-03, E-40, E-41 |
| v1.4 | 2026-11-17 | D6 에 관리자 수동 추첨(D5-1)과 추첨 후 늦은 응모 `LOST` 저장(스냅샷 행 FOR SHARE 직렬화) 추가 (사용자 결정) | E-48, phase-5 구현 상세 v1.14 |

> 이 문서는 여러 기능에 걸친 **비기능 결정**의 합의 기록이다.
> 기능 설계 문서(응모, 쿠폰, 추첨, 대기열)는 모두 이 결정을 전제로 한다.

---

## 결정 요약

| 결정점 | 선택 | 포기한 대안 |
|--------|------|-------------|
| D1. 선착순 판정 위치 / 진실의 원천 | **Redis Lua 원자 판정 + Kafka 비동기 DB 저장** | Redis 판정 + DB 동기 저장, DB 락 판정 |
| D2. Kafka 발행 실패 시 | **동기 발행 확인(producer 내부 멱등 재시도, 3초 제한), 실패 시 요청 ID 기준 Redis 보상(롤백) 후 실패 응답 + 저장 전 Redis 저장 확정(claim). 보상과 저장 확정은 Redis 에서 원자적으로 경합** | 보정 배치(Reconciliation), 애플리케이션 재시도, 추첨 전 집합 대조 |
| D3. 입장 토큰 검증 | **entry-service 가 공유 Redis 를 직접 조회 (응모 Lua 안에서)** | queue REST 호출, 서명 토큰(JWT) |
| D4. 드로우 정보 전파 | **Kafka `DropCreated` 이벤트로 entry/queue/ranking 에 복제 (커밋 후 발행, 재발행 API)** | 필요 시 REST 조회, Outbox |
| D5. 추첨 위치 | **entry-service 내부** | 별도 draw-service |
| D6. 추첨 명단 기준 | **DB 기준. 마감 후 Redis 응모 수 == DB 응모 수 가 될 때까지 대기 후 추첨** | Redis 명단으로 즉시 추첨 |
| D7. 추첨 공정성 | **시드 저장 셔플 (재현 가능)** | 단순 무작위 |

---

## D1. 선착순 판정: Redis Lua + Kafka 비동기 저장

**쉬운 설명**
> 줄 세우기(누가 몇 번째인지, 자리가 남았는지)는 아주 빠른 메모장(Redis)이 한 번에 판정하고,
> 공식 장부(DB) 기록은 우체통(Kafka)에 넣어두면 담당자가 차례로 옮겨 적어요.

**상세 설명**
- "중복 확인 → 잔여 수량 확인 → 차감 → 응모자 등록"을 **하나의 Lua 스크립트**로 실행한다.
  Redis 는 단일 스레드로 스크립트를 실행하므로 중간에 다른 요청이 끼어들 수 없다 → NFR-01, NFR-02 충족.
- 판정 성공 시 `EntryCreated` 이벤트를 Kafka 로 발행하고, 컨슈머가 DB 에 저장한다.
- DB 는 `UNIQUE(drop_id, user_id)` 로 **최종 방어선**을 갖는다 (컨슈머 재처리 시 멱등성).

**트레이드오프**
- ✅ 피크 순간 DB 락 경합 없음, 응답 지연에 DB 쓰기 미포함
- ❌ "Redis 성공 ↔ DB 기록" 사이에 시간차·유실 가능성 → D2, D6 로 보완
- ❌ Redis 자체 유실(재시작) 시 판정 상태가 사라짐 → **Redis AOF(appendonly) 활성화 필요** (구현 시 docker-compose 반영)

---

## D2. Kafka 발행 실패 대응: 동기 확인 후 Redis 롤백

**쉬운 설명**
> 장부 기록을 우체통에 못 넣으면, 줄에서 잡아준 자리도 다시 풀어주고 "실패했어요, 다시 시도해 주세요"라고 답해요.
> 그래서 "성공"이라고 답한 응모는 반드시 장부에 남아요.

**상세 설명**
0. 요청마다 **요청 ID(`eventId`, UUID)** 를 판정 **전에** 만든다. 이 값이 그대로 `EntryCreated.eventId` 가 된다.
1. Lua 판정 성공 → `applicants` 에 등록하고 `entry:{dropId}:requests` Hash 에 `userId → eventId` 를 기록한다 (어느 요청이 이 자리를 잡았는지)
2. `kafkaTemplate.send(...).get(3.5s)` 로 **브로커 수신 확인(acks=all)까지 동기 대기**
   - 재시도는 **producer 내부 멱등 재시도**만 사용한다: `enable.idempotence=true`, `retries=MAX`, `request.timeout.ms=1000`, `delivery.timeout.ms=3000`
   - **애플리케이션 재시도는 하지 않는다.** 애플리케이션이 같은 내용을 다시 `send` 하면 Kafka 는 별개 레코드로 받아 멱등 producer 도 중복을 막지 못하고, 앞선 send 가 보상 이후 뒤늦게 기록되는 **유령 이벤트**가 생기기 쉽다
   - `delivery.timeout.ms` 로 전체 시간을 3초로 제한해 send 결과가 반드시 확정되게 한다 (`get` 타임아웃만 걸면 producer 가 기본 120초 동안 계속 재시도한다)
3. 실패(예외 또는 3.5초 초과) 시 **보상 Lua** 를 같은 `eventId` 로 실행한다. 결과는 셋 중 하나 (응모 번호 `seq` 는 어느 경우에도 되돌리지 않음):
   | 결과 | 조건 | 동작 | 응답 |
   |------|------|------|------|
   | `ALREADY_PERSISTED` | `persisted` 에 userId 있음 (컨슈머가 이미 저장 확정) | 되돌리지 않음 | **202 성공** (판정 때 받은 entrySeq) — 실제로 기록됐으므로 불변식과 맞다 |
   | `NOT_OURS` | `requests[userId] ≠ eventId` (다른 요청의 응모이거나 판정이 실행되지 않음) | 되돌리지 않음 | 503 |
   | `COMPENSATED` | 그 외 | `SREM applicants` + `HDEL requests` + `INCR remaining` | 503 |
4. 클라이언트에 실패 응답 (재시도 가능 에러, 503). 보상 자체가 실패해도 로그를 남기고 503.

**저장 컨슈머의 저장 확정 (claim Lua)** — DB 저장 **전에** 호출한다 (예전 `SISMEMBER` 확인을 대체)
- `applicants` 에 userId 가 없으면 0 → 저장하지 않음 (보상된 응모의 유령 이벤트)
- `requests[userId] ≠ 메시지의 eventId` 면 0 → 저장하지 않음 (보상 후 재응모한 사용자의 **이전 시도** 유령 이벤트)
- 그 외에는 `SADD entry:{dropId}:persisted userId` 후 1 → `ON CONFLICT DO NOTHING` 저장. 재전달에도 1을 반환한다 (멱등)

**Redis 응답 실패 (타임아웃 포함)**
- 어댑터는 Redis 예외(`DataAccessException` 계열)를 포트 예외 `GateUnavailableException` 으로 감싼다 (application 이 Spring Data 예외에 의존하지 않음).
- 판정 Lua 에서 이 예외가 나면 "Lua 가 실행됐는지 모름"이다. 같은 `eventId` 로 보상을 한 번 시도한다 — 실행됐다면 `requests` 가 이 eventId 라 되돌려지고(COMPENSATED), 실행되지 않았다면 `NOT_OURS` 로 아무것도 바뀌지 않는다. 결과와 무관하게 503. (Lettuce 는 한 연결의 명령 순서를 보장하므로 늦게 도착한 판정 Lua 가 보상보다 먼저 실행된다)

> 응모(entry)와 선착순 쿠폰(coupon)이 같은 규칙을 쓴다.

**불변식**: "성공 응답을 받은 응모 = Kafka 에 반드시 기록된 응모"

**정합성 논증**: 저장 확정(claim)과 보상은 둘 다 같은 Redis 의 Lua 라 원자적으로 하나가 먼저 실행된다.
- claim 이 먼저: `persisted` 에 들어가 보상은 `ALREADY_PERSISTED` 로 거절 → DB 저장 + Redis 응모자 유지 + 성공 응답
- 보상이 먼저: `applicants`·`requests` 에서 빠져 claim 은 0 → DB 미저장 + Redis 응모자 없음 + 실패 응답
- 어느 순서든 **"DB 저장 ⇔ Redis 응모자 유지"** 가 성립하므로 풀린 자리를 다른 사용자가 가져가도 DB 응모 수가 정원을 넘지 않는다.
- `persisted` 에 있으면 반드시 `applicants` 에도 있다(보상이 `persisted` 를 되돌리지 않음). 따라서 판정이 OK 였던 요청의 보상이 `ALREADY_PERSISTED` 를 받았다면 그 저장은 이 요청의 것이다.

**남는 리스크**
- 발행은 실제로 성공했는데 타임아웃으로 실패로 판단(**유령 이벤트**) → 컨슈머 소비와 보상의 순서가 정해지지 않는다. ack 유실형에서는 소비(수십 ms)가 보상(최대 3초 뒤)보다 먼저 오는 쪽이 일반적이다.
  → ~~저장 전 `SISMEMBER` 확인~~ (v1.2): 소비가 먼저 오면 확인을 통과해 DB 에만 응모가 남고 DB 응모 수 = 정원 + 1 이 될 수 있었다 (E-40).
  → **확정 (v1.3)**: 위의 claim Lua + 3결과 보상으로 두 순서 모두 정합성을 지킨다 (정합성 논증 참고).
  → 쿠폰은 추첨의 건수 비교(D6) 같은 사후 안전장치가 없어서, 이 처리가 없으면 유령 이벤트가 곧 **초과 발급**이 된다. 응모·쿠폰 모두 필수 (쿠폰 적용 방식은 6단계에서 확정).
- Redis 응답 타임아웃 (E-41) → 위의 `GateUnavailableException` 처리로 되돌린다. **보상까지 실패하면** 그 사용자는 Redis 에만 응모돼 있고 Kafka 메시지가 없다 → 언더 응모 + D6 건수 불일치(추첨 대기). 로그(알림) 대상이며 운영 대응 또는 5단계 대조로 처리한다.
- **보상 후 응모 번호 재사용** (해결됨): 번호를 `M - remaining` 으로 계산하면 보상 후 같은 번호가 재발급되어 정상 응모가 DB 유니크 제약에서 누락될 수 있다.
  → `entrySeq` 를 **되돌리지 않는 별도 카운터(`INCR entry:{dropId}:seq`)** 로 발급. 빈 번호는 허용한다 (번호는 정렬 기준일 뿐 당첨은 셔플로 결정).
- 보상 Lua 자체가 실패하는 극단 케이스 (발행 실패 경로) → 정원 한 자리가 "새는" 상태. 오버 응모가 아니라 **언더 응모** 방향이므로 정합성 위반은 아님.
- 저장 컨슈머 자체 실패(DB 장애 등) → 3회 재시도 후 `{topic}.DLT` 로 보낸다 (`entry.created.DLT`, `coupon.issued.DLT`). 응모는 Redis 에 있고 DB 에 없는 상태라 D6 건수 비교에서 드러나며, DLT 재처리로 복구한다. claim 후 DB 저장이 실패한 경우도 같다 — `persisted` 에 있어 보상되지 않으므로, v1.3 이후 남는 Redis·DB 불일치는 **이 경우뿐**이다 (재처리 시 claim 은 멱등하게 1).
- 새 키 `requests`·`persisted` 의 TTL 은 다른 응모 키와 함께 5단계에서 정한다 (E-42).

---

## D3. 입장 토큰 검증: 공유 Redis 직접 조회

**쉬운 설명**
> 대기열이 "입장 팔찌"를 같은 명부(Redis)에 적어두면, 응모 창구는 그 명부만 보고 확인해요. 매번 대기열 창구에 전화할 필요가 없어요.

**상세 설명**
- queue-service 가 `queue:{dropId}:token:{userId}` 를 TTL 과 함께 기록한다 (**쓰기 소유자: queue-service**).
- entry-service 는 응모 Lua 안에서 `EXISTS` 로만 확인한다 (**읽기 전용**).
- 토큰은 응모 후 삭제하지 않는다. 같은 드로우 재응모는 중복 방지(applicants Set)가 막고, 토큰은 TTL 로 자연 만료.
- 반대로 `entry:{dropId}:*` 키(`applicants`, `requests`, `persisted` 등)의 쓰기 소유자는 entry-service 하나다. queue-service 는 `remaining` 만 읽는다.

**트레이드오프**
- ✅ 피크 순간 서비스 간 동기 호출 없음, 토큰 확인이 판정과 원자적으로 묶임
- ❌ 서비스 간 저장소 공유 → **키 네임스페이스별 쓰기 소유자를 문서로 고정**하여 완화 ([architecture.md](../02-architecture/architecture.md) 참고)

---

## D4. 드로우 정보 전파: Kafka 이벤트 복제

**상세 설명**
- product-service 가 드로우 등록 시 `DropCreated` 발행.
- entry-service: `drop_snapshots` 테이블 저장 + Redis 응모 메타/잔여 수량 초기화
- queue-service: Redis 대기열 메타(오픈/마감 시각) 초기화
- ranking-service: 랭킹 표시용 드로우 이름 저장

**발행 방식 (확정)**: 커밋 후 발행 + 관리자 재발행 API. Outbox 는 사용하지 않는다.
- application 은 포트(`DropEventPublisher`)만 호출하고 발행 시점을 모른다.
- infrastructure 발행 어댑터가 `TransactionSynchronizationManager` 로 **`afterCommit` 에 발행을 등록**한다. 활성 트랜잭션이 없으면 즉시 발행한다. (회원가입 `user.registered` 도 같은 방식)
- 롤백되면 발행되지 않는다. 커밋 후 발행이 최종 실패하면 ERROR 로그를 남기고, 관리자 재발행 API 로 복구한다.
- 수신 측(entry/queue/ranking)은 재수신에 멱등해야 한다 (`SET NX`, `ON CONFLICT DO NOTHING`).
- 수신 측 저장 실패: entry 의 `drop.created` 처리는 3회 재시도 후 `drop.created.DLT`. queue·ranking 은 Redis 쓰기만 하므로 3회 재시도 후 로그.

**리스크**: 커밋 후 발행 실패(이중 쓰기) → 드로우는 있는데 응모 불가. 관리자 저빈도 작업이라 재발행 API 로 감당하고, 더 확실하게 하려면 Outbox 로 전환한다.
- 드로우 **수정**은 1차 범위에서 제외 (등록 후 변경 불가) → 동기화 문제 회피.

---

## D6. 추첨 명단: DB 기준 + 건수 일치 대기

**쉬운 설명**
> 마감 종이 울리면 줄을 닫고, 장부 정리가 다 끝났는지(줄 선 사람 수 == 장부 기록 수) 확인한 다음 추첨해요.

**상세 설명**
1. 마감 시각 도달 → Redis 응모 상태를 `CLOSED` 로 변경 (이후 응모 Lua 거부)
2. `SCARD entry:{dropId}:applicants` (Redis 응모 수) 와 `COUNT(entries)` (DB 응모 수) 비교
3. 일치할 때까지 대기 → 다음 스케줄러 주기에 재확인. 일정 시간 초과 시 알림(로그) 후 계속 대기
4. 일치하면 DB 응모자로 추첨
5. 일치하지 않은 채 오래 걸리면 경고 후 관리자 수동 추첨 (DB 응모 기준, 사용자 결정 D5-1). 이 경우 추첨 뒤에 늦게 저장되는 응모(DLT 재처리·lag)는 **`LOST` 로 저장**한다 — 저장 트랜잭션이 `drop_snapshots` 행을 `FOR SHARE` 로 잠가 추첨 트랜잭션의 첫 UPDATE(`CLOSING → DRAWN`)와 직렬화하므로, 응모는 "추첨 대상에 포함" 또는 "LOST 저장" 둘 중 하나가 되고 `APPLIED` 로 남지 않는다 (E-48, 사용자 결정)

---

## D7. 추첨 공정성: 시드 저장 셔플

**상세 설명**
- 응모자를 `entry_seq` 오름차순으로 정렬 (결정적 순서 확보)
- `SecureRandom` 으로 시드 생성 → `new Random(seed)` 로 Fisher–Yates 셔플 → 앞 K명 당첨
- 시드를 `draw_results` 에 저장 → **같은 응모자 목록 + 같은 시드 = 같은 결과** 재현 가능 (테스트·감사 용이)
