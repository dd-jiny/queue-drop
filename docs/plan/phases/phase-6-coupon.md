# 6단계 — 선착순 쿠폰 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 대기열 없이 선착순으로 쿠폰을 1인 1매, 총 수량까지 발급한다. 피크 요청이 그대로 Redis 에 도달해도 초과·중복 발급이 없어야 한다 |
| 요구사항 | REQ-COUPON-01 ~ 05, NFR-01 ~ 03 |
| 브랜치 | `feat/phase-6-coupon` |
| 대상 서비스 | coupon-service |

**선행 조건**
- 0단계: `common` 이벤트(`CouponIssued`)·`Topics`, ArchUnit testFixtures, Testcontainers 지원, Flyway, 공통 에러 응답(`ErrorCode`, `@RestControllerAdvice` 틀)
- 1단계: Gateway 가 `X-User-Id`, `X-User-Role` 주입
- 4단계는 **필수 아님** (독립 단계). 단, 4단계가 먼저 끝났다면 Lua 실행·동기 발행·보상 코드를 참고한다 (6장 D6-1)

**참고 설계 문서**
- [coupon-issue.md](../../design/05-features/coupon-issue.md) — 쿠폰 설계 본문
- [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D1, D2
- [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.4
- [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 3장, 5.1 ~ 5.3
- [erd.md](../../design/06-erd/erd.md) coupon_db
- [architecture.md](../../design/02-architecture/architecture.md) 4장(토픽), 5장(Redis 키)
- [layered-architecture.md](../../design/02-architecture/layered-architecture.md)

---

## 2. 산출물 파일 목록

```
coupon-service/src/main/java/com/queuedrop/coupon/
├── domain
│   ├── model
│   │   ├── CouponEvent.java                     [신규]
│   │   ├── IssuedCoupon.java                    [신규]
│   │   └── IssueOutcome.java                    [신규]
│   └── repository
│       ├── CouponEventRepository.java           [신규]
│       └── IssuedCouponRepository.java          [신규]
├── application
│   ├── service
│   │   ├── CouponEventService.java              [신규] 등록, 재초기화
│   │   ├── CouponIssueService.java              [신규] 발급 (응답 경로)
│   │   ├── CouponSaveService.java               [신규] 저장 (컨슈머 경로)
│   │   └── CouponQueryService.java              [신규]
│   ├── dto
│   │   ├── CouponEventRegisterCommand.java      [신규]
│   │   ├── CouponIssueCommand.java              [신규]
│   │   ├── CouponSaveCommand.java               [신규]
│   │   ├── CouponEventResult.java               [신규]
│   │   ├── CouponIssueResult.java               [신규]
│   │   ├── MyCouponResult.java                  [신규]
│   │   ├── GateResult.java                      [신규]
│   │   └── CouponIssuedPayload.java             [신규]
│   ├── exception
│   │   ├── CouponErrorCode.java                 [신규]
│   │   └── CouponException.java                 [신규]
│   └── port
│       ├── CouponGate.java                      [신규]
│       └── CouponEventPublisher.java            [신규]
├── infrastructure
│   ├── persistence
│   │   ├── CouponEventJpaEntity.java            [신규]
│   │   ├── IssuedCouponJpaEntity.java           [신규]
│   │   ├── CouponEventJpaRepository.java        [신규]
│   │   ├── IssuedCouponJpaRepository.java       [신규]
│   │   ├── CouponEventRepositoryImpl.java       [신규]
│   │   ├── IssuedCouponRepositoryImpl.java      [신규]
│   │   └── CouponMapper.java                    [신규]
│   ├── redis
│   │   ├── CouponKeys.java                      [신규]
│   │   └── RedisCouponGate.java                 [신규]
│   ├── messaging
│   │   └── KafkaCouponEventPublisher.java       [신규]
│   └── config
│       └── ClockConfig.java                     [신규]
└── presentation
    ├── api
    │   ├── CouponEventController.java           [신규]
    │   ├── CouponController.java                [신규]
    │   ├── request/CouponEventRegisterRequest.java
    │   └── response/{CouponEventResponse, CouponIssueResponse, MyCouponResponse}.java
    └── listener
        └── CouponIssuedListener.java            [신규]

coupon-service/src/main/resources/
├── scripts/issue.lua                            [신규]
├── scripts/compensate.lua                       [신규]
├── db/migration/V1__create_coupon_events.sql    [신규]
├── db/migration/V2__create_issued_coupons.sql   [신규]
└── application.yml                              [수정] Kafka producer, coupon.* 설정

gateway/src/main/resources/application.yml       [수정] 인증 제외 경로에 GET /api/coupons/events/* 추가 (6장 설계 수정 필요)
```

---

## 3. 구현 상세

### 3.1 domain

#### `CouponEvent` — 쿠폰 이벤트
```java
package com.queuedrop.coupon.domain.model;

public final class CouponEvent {
    private final Long id;
    private final String name;
    private final int totalQuantity;
    private final Instant issueStartAt;
    private final Instant createdAt;

    public static CouponEvent create(String name, int totalQuantity, Instant issueStartAt, Instant now) {
        if (name == null || name.isBlank()) throw new DomainException("name is required");
        if (totalQuantity < 1) throw new DomainException("totalQuantity must be >= 1");
        if (!issueStartAt.isAfter(now)) throw new DomainException("issueStartAt must be in the future");
        return new CouponEvent(null, name, totalQuantity, issueStartAt, now);
    }

    public boolean isStarted(Instant now) { return !now.isBefore(issueStartAt); }
    // getter ...
}
```
- `issueStartAt` 이 미래여야 하는 이유: 등록 직후 Redis 초기화가 끝나기 전에 발급이 시작되는 것을 막는다 (구현 결정).

#### `IssuedCoupon`, `IssueOutcome`
```java
public final class IssuedCoupon {
    private final Long id;
    private final Long couponEventId;
    private final Long userId;
    private final Instant issuedAt;

    public static IssuedCoupon issue(Long couponEventId, Long userId, Instant issuedAt) { /* ... */ }
}

public enum IssueOutcome {
    OK, NOT_STARTED, DUPLICATE, SOLD_OUT,
    NOT_FOUND        // Redis 에 이벤트 meta 가 없음 (구현 결정: 미등록 또는 초기화 실패)
}
```
> "1인 1매, 총 수량 초과 금지" 는 도메인 규칙이지만 원자성을 위해 판정 자체는 Redis Lua 에 위임한다. domain 에는 결과 타입만 둔다 ([layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.3).

#### Repository 포트
```java
public interface CouponEventRepository {
    CouponEvent save(CouponEvent event);
    Optional<CouponEvent> findById(Long id);
}

public interface IssuedCouponRepository {
    /** UK(coupon_event_id, user_id) 충돌 시 저장하지 않고 false */
    boolean saveIfAbsent(IssuedCoupon coupon);
    List<IssuedCoupon> findByUserId(Long userId);
}
```

---

### 3.2 application

#### 포트
```java
package com.queuedrop.coupon.application.port;

public interface CouponGate {
    void initialize(Long eventId, int totalQuantity, Instant issueStartAt);   // SET NX, 멱등
    GateResult tryIssue(Long eventId, Long userId, Instant now);
    void compensate(Long eventId, Long userId);
    boolean isIssued(Long eventId, Long userId);
    long remaining(Long eventId);
}

public interface CouponEventPublisher {
    void publishIssued(CouponIssuedPayload payload);    // 브로커 ack 까지 동기 대기, 실패 시 예외
}
```

#### DTO
```java
public record CouponEventRegisterCommand(String name, int totalQuantity, Instant issueStartAt, String requesterRole) {}
public record CouponIssueCommand(Long eventId, Long userId) {}
public record CouponSaveCommand(Long couponEventId, Long userId, Instant issuedAt) {}

public record GateResult(IssueOutcome outcome) {}        // application 내부 전용 (포트 반환)
public record CouponIssuedPayload(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}

public record CouponEventResult(Long id, String name, int totalQuantity, long remaining, Instant issueStartAt) {}
public record CouponIssueResult(Long couponEventId, Instant issuedAt) {}
public record MyCouponResult(Long couponEventId, String eventName, Instant issuedAt) {}
```
- `GateResult` 는 domain enum 을 담지만 **포트 반환 타입**이라 application 안에서만 쓰인다. 서비스 public 메서드는 `CouponIssueResult` 만 반환한다 (ArchUnit R6).
- `MyCouponResult.eventName` 을 위해 조회 시 `coupon_events` 를 함께 읽는다 (같은 서비스 DB).

#### 에러 코드
```java
public enum CouponErrorCode implements ErrorCode {
    EVENT_NOT_FOUND(404, "쿠폰 이벤트를 찾을 수 없습니다"),
    NOT_STARTED(409, "아직 발급 시작 전입니다"),
    DUPLICATE(409, "이미 발급받은 쿠폰입니다"),
    SOLD_OUT(409, "쿠폰이 모두 소진되었습니다"),
    TEMPORARILY_FAILED(503, "일시적으로 발급에 실패했습니다. 다시 시도해 주세요"),
    INVALID_EVENT(400, "쿠폰 이벤트 정보가 올바르지 않습니다"),
    FORBIDDEN(403, "권한이 없습니다");
    // ...
}
```

#### `CouponEventService` — 등록 · 재초기화
```java
@Service
@RequiredArgsConstructor
public class CouponEventService {
    private final CouponEventRepository eventRepository;
    private final CouponGate gate;
    private final Clock clock;

    /** 트랜잭션을 열지 않는다: INSERT 1건은 그 자체로 원자적, Redis 초기화는 저장 후 (구현 결정) */
    public CouponEventResult register(CouponEventRegisterCommand command) {
        requireAdmin(command.requesterRole());
        CouponEvent event;
        try {
            event = CouponEvent.create(command.name(), command.totalQuantity(), command.issueStartAt(), clock.instant());
        } catch (DomainException e) {
            throw new CouponException(CouponErrorCode.INVALID_EVENT, e.getMessage());
        }
        CouponEvent saved = eventRepository.save(event);          // 커밋 완료
        try {
            gate.initialize(saved.getId(), saved.getTotalQuantity(), saved.getIssueStartAt());
        } catch (RuntimeException e) {
            log.warn("coupon redis init failed eventId={} — reinitialize required", saved.getId(), e);
        }
        return toResult(saved, gate.remaining(saved.getId()));
    }

    /** 관리자 재초기화: SET NX 라 진행 중인 잔여 수량을 덮어쓰지 않는다 */
    public void reinitialize(Long eventId, String requesterRole) {
        requireAdmin(requesterRole);
        CouponEvent event = eventRepository.findById(eventId)
                .orElseThrow(() -> new CouponException(CouponErrorCode.EVENT_NOT_FOUND));
        gate.initialize(event.getId(), event.getTotalQuantity(), event.getIssueStartAt());
    }
}
```
- "커밋 후 Redis 초기화" 를 `@TransactionalEventListener` 없이 **트랜잭션을 열지 않는 방식**으로 구현했다. 저장이 단일 INSERT 라 가능하다 (구현 결정).

#### `CouponIssueService` — 발급 (응답 경로, DB 미사용)
```java
@Service
@RequiredArgsConstructor
public class CouponIssueService {
    private final CouponGate gate;
    private final CouponEventPublisher publisher;
    private final Clock clock;

    public CouponIssueResult issue(CouponIssueCommand command) {
        Instant now = clock.instant();
        GateResult result = gate.tryIssue(command.eventId(), command.userId(), now);

        switch (result.outcome()) {
            case OK -> { }                                                      // 아래로 진행
            case NOT_STARTED -> throw new CouponException(CouponErrorCode.NOT_STARTED);
            case DUPLICATE   -> throw new CouponException(CouponErrorCode.DUPLICATE);
            case SOLD_OUT    -> throw new CouponException(CouponErrorCode.SOLD_OUT);
            case NOT_FOUND   -> throw new CouponException(CouponErrorCode.EVENT_NOT_FOUND);
        }

        CouponIssuedPayload payload = new CouponIssuedPayload(
                UUID.randomUUID().toString(), command.eventId(), command.userId(), now);
        try {
            publisher.publishIssued(payload);                   // ack 까지 대기 (재시도 포함)
        } catch (RuntimeException e) {
            gate.compensate(command.eventId(), command.userId());   // D2: Redis 보상
            throw new CouponException(CouponErrorCode.TEMPORARILY_FAILED);
        }
        return new CouponIssueResult(command.eventId(), now);
    }
}
```
- 의존 그래프에 `IssuedCouponRepository` 가 없다 — 응답 경로에서 DB 를 뺀 설계가 그대로 드러난다.

#### `CouponSaveService` — 저장 (컨슈머 경로)
```java
@Service
@RequiredArgsConstructor
public class CouponSaveService {
    private final IssuedCouponRepository issuedRepository;
    private final CouponGate gate;

    public void save(CouponSaveCommand command) {
        // 보상된 발급(유령 이벤트) 걸러내기 — 없으면 보상으로 되돌아간 수량을 다른 사람이 받아 초과 발급이 된다
        if (!gate.isIssued(command.couponEventId(), command.userId())) {
            log.info("skip compensated coupon eventId={} userId={}", command.couponEventId(), command.userId());
            return;
        }
        boolean saved = issuedRepository.saveIfAbsent(
                IssuedCoupon.issue(command.couponEventId(), command.userId(), command.issuedAt()));
        if (!saved) {
            log.debug("duplicate coupon.issued ignored ...");      // 재전달 (멱등)
        }
    }
}
```
> 응모(4단계)는 유령 이벤트가 저장되면 "응모 수 불일치" 로 드러나지만, 쿠폰은 건수 비교 단계가 없어 **조용히 초과 발급**이 된다. 그래서 저장 전 Redis 확인이 쿠폰에서는 필수다 (6장 설계 수정 필요).

#### `CouponQueryService`
```java
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CouponQueryService {
    public CouponEventResult event(Long eventId) { /* DB 이벤트 + gate.remaining(eventId) */ }
    public List<MyCouponResult> myCoupons(Long userId) { /* issuedRepository.findByUserId + 이벤트 이름 */ }
}
```

---

### 3.3 infrastructure

#### Redis 키 (`CouponKeys`)
| 키 | 타입 | 값 |
|----|------|----|
| `coupon:{eventId}:meta` | Hash | `issueStartAt` (epoch ms) |
| `coupon:{eventId}:remaining` | String | 잔여 수량 |
| `coupon:{eventId}:issued` | Set | 발급받은 userId |

> 세 키 모두 같은 eventId 를 쓴다. Redis Cluster 전환 시에는 `coupon:{eventId}` 의 `{}` 해시태그로 같은 슬롯에 배치되므로 Lua 다중 키 실행이 가능하다.

#### `scripts/issue.lua`
```lua
-- KEYS[1] = coupon:{eventId}:meta
-- KEYS[2] = coupon:{eventId}:remaining
-- KEYS[3] = coupon:{eventId}:issued
-- ARGV[1] = userId
-- ARGV[2] = nowEpochMs (애플리케이션 시각, 비결정적 TIME 명령 사용 금지)

local startAt = redis.call('HGET', KEYS[1], 'issueStartAt')
if not startAt then
  return 'NOT_FOUND'
end
if tonumber(ARGV[2]) < tonumber(startAt) then
  return 'NOT_STARTED'
end
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
  return 'DUPLICATE'
end
local remaining = tonumber(redis.call('GET', KEYS[2]) or '0')
if remaining <= 0 then
  return 'SOLD_OUT'
end
redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])
return 'OK'
```

#### `scripts/compensate.lua`
```lua
-- KEYS[1] = coupon:{eventId}:remaining
-- KEYS[2] = coupon:{eventId}:issued
-- ARGV[1] = userId
-- SREM 이 실제로 지운 경우에만 수량 복구 → 보상이 두 번 실행돼도 수량이 늘지 않음
if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
  redis.call('INCR', KEYS[1])
  return 1
end
return 0
```

#### `RedisCouponGate`
```java
@Component
@RequiredArgsConstructor
public class RedisCouponGate implements CouponGate {
    private final StringRedisTemplate redis;
    private final RedisScript<String> issueScript;       // scripts/issue.lua (EVALSHA)
    private final RedisScript<Long> compensateScript;    // scripts/compensate.lua

    @Override
    public void initialize(Long eventId, int totalQuantity, Instant issueStartAt) {
        redis.opsForValue().setIfAbsent(CouponKeys.remaining(eventId), String.valueOf(totalQuantity));   // SET NX
        redis.opsForHash().putIfAbsent(CouponKeys.meta(eventId), "issueStartAt",
                String.valueOf(issueStartAt.toEpochMilli()));                                          // HSETNX
    }

    @Override
    public GateResult tryIssue(Long eventId, Long userId, Instant now) {
        String r = redis.execute(issueScript,
                List.of(CouponKeys.meta(eventId), CouponKeys.remaining(eventId), CouponKeys.issued(eventId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()));
        return new GateResult(IssueOutcome.valueOf(r));
    }

    @Override
    public void compensate(Long eventId, Long userId) {
        redis.execute(compensateScript,
                List.of(CouponKeys.remaining(eventId), CouponKeys.issued(eventId)), String.valueOf(userId));
    }

    @Override public boolean isIssued(Long eventId, Long userId) { /* SISMEMBER */ }
    @Override public long remaining(Long eventId) { /* GET, 없으면 0 */ }
}
```

#### `KafkaCouponEventPublisher`
```java
@Component
@RequiredArgsConstructor
public class KafkaCouponEventPublisher implements CouponEventPublisher {
    private static final int MAX_ATTEMPTS = 3;           // 최초 1 + 재시도 2 (D2)
    private final KafkaTemplate<String, Object> kafka;

    @Override
    public void publishIssued(CouponIssuedPayload p) {
        CouponIssued message = new CouponIssued(p.eventId(), p.couponEventId(), p.userId(), p.issuedAt());
        String key = p.couponEventId() + ":" + p.userId();
        RuntimeException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                kafka.send(Topics.COUPON_ISSUED, key, message).get(3, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                last = new EventPublishException("coupon.issued", e);
            }
        }
        throw last;
    }
}
```
- 파티션 키 `couponEventId:userId` ([architecture.md](../../design/02-architecture/architecture.md) 4장).
- producer 설정은 4단계와 동일 (`application.yml`):
```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        delivery.timeout.ms: 3000
        request.timeout.ms: 2000
```

#### Flyway
`V1__create_coupon_events.sql`
```sql
CREATE TABLE coupon_events (
    id              BIGSERIAL     PRIMARY KEY,
    name            VARCHAR(100)  NOT NULL,
    total_quantity  INT           NOT NULL,
    issue_start_at  TIMESTAMPTZ   NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL,
    CONSTRAINT ck_coupon_events_quantity CHECK (total_quantity >= 1)
);
```

`V2__create_issued_coupons.sql`
```sql
CREATE TABLE issued_coupons (
    id               BIGSERIAL    PRIMARY KEY,
    coupon_event_id  BIGINT       NOT NULL REFERENCES coupon_events (id),
    user_id          BIGINT       NOT NULL,
    issued_at        TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uk_issued_coupons_event_user UNIQUE (coupon_event_id, user_id)
);

CREATE INDEX idx_issued_coupons_user ON issued_coupons (user_id, issued_at DESC);   -- 내 쿠폰 조회
```

#### `IssuedCouponJpaRepository` — UK 위반 무시
```java
public interface IssuedCouponJpaRepository extends JpaRepository<IssuedCouponJpaEntity, Long> {

    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO issued_coupons (coupon_event_id, user_id, issued_at)
        VALUES (:eventId, :userId, :issuedAt)
        ON CONFLICT (coupon_event_id, user_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") Long eventId, @Param("userId") Long userId, @Param("issuedAt") Instant issuedAt);

    List<IssuedCouponJpaEntity> findByUserIdOrderByIssuedAtDesc(Long userId);
}
```
- `DataIntegrityViolationException` 을 잡는 대신 `ON CONFLICT DO NOTHING` 을 쓴다 (구현 결정): 예외로 트랜잭션이 rollback-only 가 되는 문제를 피하고, 영향 행 수(0/1)로 결과를 바로 안다.

---

### 3.4 presentation

#### API
| Method | Path | 권한 | 요청 | 응답 |
|--------|------|------|------|------|
| POST | `/api/coupons/events` | ADMIN | `{name, totalQuantity, issueStartAt}` | 201 `CouponEventResponse` |
| GET | `/api/coupons/events/{eventId}` | 공개 | - | 200 `{id, name, totalQuantity, remaining, issueStartAt}` |
| POST | `/api/coupons/events/{eventId}/issue` | USER | - | 202 `{couponEventId, issuedAt}` |
| GET | `/api/coupons/me` | USER | - | 200 `[{couponEventId, eventName, issuedAt}]` |
| POST | `/api/coupons/events/{eventId}/initialize` | ADMIN | - | 204 (재초기화, 6장 설계 수정 필요) |

#### `CouponEventController`, `CouponController`
```java
@RestController
@RequestMapping("/api/coupons/events")
@RequiredArgsConstructor
public class CouponEventController {
    private final CouponEventService eventService;
    private final CouponQueryService queryService;

    @PostMapping
    public ResponseEntity<CouponEventResponse> register(@RequestHeader("X-User-Role") String role,
                                                        @Valid @RequestBody CouponEventRegisterRequest request) {
        CouponEventResult result = eventService.register(request.toCommand(role));
        return ResponseEntity.status(HttpStatus.CREATED).body(CouponEventResponse.from(result));
    }

    @GetMapping("/{eventId}")
    public CouponEventResponse get(@PathVariable Long eventId) {
        return CouponEventResponse.from(queryService.event(eventId));
    }

    @PostMapping("/{eventId}/initialize")
    public ResponseEntity<Void> reinitialize(@RequestHeader("X-User-Role") String role, @PathVariable Long eventId) {
        eventService.reinitialize(eventId, role);
        return ResponseEntity.noContent().build();
    }
}

@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {
    private final CouponIssueService issueService;
    private final CouponQueryService queryService;

    @PostMapping("/events/{eventId}/issue")
    public ResponseEntity<CouponIssueResponse> issue(@RequestHeader("X-User-Id") Long userId, @PathVariable Long eventId) {
        CouponIssueResult result = issueService.issue(new CouponIssueCommand(eventId, userId));
        return ResponseEntity.accepted().body(CouponIssueResponse.from(result));
    }

    @GetMapping("/me")
    public List<MyCouponResponse> me(@RequestHeader("X-User-Id") Long userId) {
        return queryService.myCoupons(userId).stream().map(MyCouponResponse::from).toList();
    }
}
```
- `CouponEventRegisterRequest`: `@NotBlank name`, `@Min(1) totalQuantity`, `@NotNull @Future issueStartAt`.
- `@RestControllerAdvice` 는 0단계 공통 틀에 `CouponException` → `CouponErrorCode.status` 매핑을 등록한다.
- `/me` 는 DB 기준이라 발급 직후(컨슈머 처리 전)에는 비어 있을 수 있다. 응모처럼 Redis 로 보완하지 않는 이유: 쿠폰 목록은 이벤트 이름·발급 시각이 필요한데 Redis Set 에는 userId 만 있다 (구현 결정, 6장 리스크).

#### `CouponIssuedListener`
```java
@Component
@RequiredArgsConstructor
public class CouponIssuedListener {
    private final CouponSaveService saveService;

    @KafkaListener(topics = Topics.COUPON_ISSUED, groupId = "coupon-service")
    public void on(CouponIssued message) {
        saveService.save(new CouponSaveCommand(message.couponEventId(), message.userId(), message.issuedAt()));
    }
}
```

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ/NFR |
|----|-----------|---------------|-------------------------------|---------------|
| T6-01 | domain 단위 | `CouponEventTest` | G: 수량 0 / 빈 이름 / 시작 시각 과거 / W: `create` / T: 각각 `DomainException` | REQ-COUPON-01 |
| T6-02 | domain 단위 | `CouponEventTest` | G: 시작 12:00 / W: `isStarted(11:59:59)`, `(12:00)` / T: false, true | REQ-COUPON-02 |
| T6-03 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Fake Gate OK, Fake publisher 성공 / W: `issue` / T: 결과 반환, 발행 1건 | REQ-COUPON-02 |
| T6-04 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Gate 가 NOT_STARTED / DUPLICATE / SOLD_OUT / NOT_FOUND / W: `issue` / T: 각 에러 코드 예외, 발행 없음 | REQ-COUPON-02, 03 |
| T6-05 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Fake publisher 예외 / W: `issue` / T: `TEMPORARILY_FAILED`, Gate 에 compensate 1회 | NFR-03 |
| T6-06 | application 단위 (Fake) | `CouponSaveServiceTest` | G: Gate 에 사용자 없음 (보상됨) / W: `save` / T: 저장 안 함 | NFR-01, 03 |
| T6-07 | application 단위 (Fake) | `CouponEventServiceTest` | G: USER 권한 / W: `register` / T: `FORBIDDEN`. G: Gate 초기화 예외 / W: `register` / T: DB 저장됨, 결과 반환, WARN 로그 | REQ-COUPON-01 |
| T6-08 | infra 통합 (Redis) | `RedisCouponGateTest` | Lua 결과 5종: 미초기화 → NOT_FOUND, 시작 전 → NOT_STARTED, 첫 발급 → OK, 재발급 → DUPLICATE, 수량 소진 → SOLD_OUT | REQ-COUPON-02, 03 |
| T6-09 | infra 통합 (Redis) | `RedisCouponGateTest` | G: 발급 1건 / W: `compensate` 2번 / T: 잔여 수량 1만 복구, Set 에서 제거 | NFR-03 |
| T6-10 | infra 통합 (Redis) | `RedisCouponGateTest` | G: 수량 100 초기화 후 30개 발급 / W: `initialize` 재호출 / T: 잔여 70 유지 (SET NX) | REQ-COUPON-01 |
| T6-11 | infra 통합 (PostgreSQL) | `IssuedCouponRepositoryImplTest` | W: 같은 (event, user) 2번 `saveIfAbsent` / T: true, false, 행 1개 | NFR-02 |
| T6-12 | infra 통합 (Kafka) | `KafkaCouponEventPublisherTest` | W: `publishIssued` / T: `coupon.issued` 토픽에 키 `eventId:userId` 메시지 1건 | - |
| T6-13 | **동시성 (전체 컨텍스트)** | `CouponIssueConcurrencyTest` | G: 수량 100, 서로 다른 사용자 1,000명 / W: 32스레드로 동시 `issue` / T: 성공 정확히 100, SOLD_OUT 900, Redis 잔여 0, Set 100, **최종 DB 100건** (Awaitility) | NFR-01 |
| T6-14 | **동시성 (전체 컨텍스트)** | `CouponIssueConcurrencyTest` | G: 수량 100, 같은 사용자 / W: 10스레드 동시 `issue` / T: 성공 1, DUPLICATE 9, DB 1건 | NFR-02 |
| T6-15 | 통합 (전체 컨텍스트) | `CouponIssueFailureTest` | G: 테스트용 `@Primary` publisher 가 항상 예외 / W: `issue` / T: 503, Redis 잔여·Set 원상복구 | NFR-03 |
| T6-16 | 통합 (전체 컨텍스트) | `CouponGhostEventTest` | G: 수량 1. A 발급 → publisher 는 실제 발행했지만 예외를 던지도록 주입 → 보상 → B 발급 성공 / W: 컨슈머 처리 완료 / T: DB 에 B 1건만, A 없음 (**초과 발급 없음**) | NFR-01, 03 |
| T6-17 | 통합 (전체 컨텍스트) | `CouponIssuedListenerTest` | G: 같은 `coupon.issued` 메시지 3번 발행 / W: 소비 / T: DB 1건 | NFR-02 |
| T6-18 | presentation 슬라이스 | `CouponControllerTest` | 에러 코드 → HTTP: NOT_STARTED/DUPLICATE/SOLD_OUT 409, EVENT_NOT_FOUND 404, TEMPORARILY_FAILED 503, 성공 202 | REQ-COUPON-02 |
| T6-19 | presentation 슬라이스 | `CouponEventControllerTest` | 요청 검증: 수량 0, 과거 시작 시각 → 400. 정상 → 201. `GET` 응답에 `remaining` 포함 | REQ-COUPON-01, 04 |
| T6-20 | 통합 (전체 컨텍스트) | `MyCouponQueryTest` | G: 이벤트 2개에서 각각 발급 / W: `GET /me` / T: 2건, 이벤트 이름 포함, 최신순 | REQ-COUPON-04 |
| T6-21 | 아키텍처 | `ArchitectureTest` | R1~R7 통과. `CouponIssueService` 가 `IssuedCouponRepository` 에 의존하지 않음 (추가 규칙, 구현 결정) | - |

---

## 5. 커밋 계획

| # | 범위 | 커밋 메시지 |
|---|------|-------------|
| 1 | domain | `feat(coupon): 쿠폰 도메인 모델 추가`<br>- CouponEvent(생성 검증, isStarted), IssuedCoupon, IssueOutcome<br>- Repository 포트 2개<br>- T6-01, T6-02 |
| 2 | application | `feat(coupon): 쿠폰 등록·발급·저장 유스케이스 구현`<br>- CouponEventService, CouponIssueService, CouponSaveService, CouponQueryService<br>- 포트 CouponGate, CouponEventPublisher, 에러 코드<br>- T6-03 ~ T6-07 (Fake 포트) |
| 3 | infra (Redis) | `feat(coupon): 선착순 발급 Lua 스크립트와 Gate 구현`<br>- issue.lua, compensate.lua, RedisCouponGate(SET NX 초기화)<br>- T6-08 ~ T6-10 |
| 4 | infra (DB, Kafka) | `feat(coupon): 쿠폰 영속성과 coupon.issued 발행 구현`<br>- V1, V2 마이그레이션, ON CONFLICT DO NOTHING 저장<br>- KafkaCouponEventPublisher (acks=all, 재시도 2회)<br>- T6-11, T6-12 |
| 5 | presentation | `feat(coupon): 쿠폰 API와 발급 이벤트 리스너 추가`<br>- CouponEventController, CouponController, CouponIssuedListener<br>- Gateway 인증 제외 경로에 쿠폰 이벤트 조회 추가<br>- T6-18 ~ T6-20 |
| 6 | 동시성·장애 테스트 | `test(coupon): 동시 발급·보상·유령 이벤트 테스트 추가`<br>- T6-13 ~ T6-17, T6-21 |
| 7 | docs | `docs: 6단계 구현 결과 반영`<br>- 설계 수정 필요 항목 반영, 플랜 진행 현황 갱신 |

---

## 6. 결정 필요 · 리스크

### 결정 필요
| ID | 항목 | 선택지 | 기본안 |
|----|------|--------|--------|
| D6-1 | 응모(4단계)와의 공통화 (Lua 실행·동기 발행·보상) | (a) **서비스별로 각자 구현** — 중복은 있지만 서비스 독립성 유지 (b) `common` 에 인프라 헬퍼(`LuaScriptRunner`, `SyncKafkaPublisher`) 추가 — common 이 Spring Data Redis·Spring Kafka 에 의존하게 됨 (c) 별도 `common-infra` 모듈 신설 — 순수 계약 모듈(common)과 분리, 모듈 1개 증가 | (a). 중복 대상이 두 서비스·클래스 2~3개 수준이라 추상화 비용이 더 크다. 8단계 이후 세 번째 사용처가 생기면 (c) 검토 |
| D6-2 | 매진 후 요청 차단 | (a) 그대로 Lua 실행 (O(1)) (b) 애플리케이션 로컬 "매진" 플래그로 Redis 호출 전 차단 | (a). 8단계 부하 테스트에서 매진 구간 Redis 부하를 측정한 뒤 결정 |
| D6-3 | `/me` 발급 직후 미반영 | (a) DB 만 조회 (잠깐 비어 보일 수 있음) (b) Redis Set 으로 "발급됨" 표시 보완 | (a). 발급 응답(202)에 이미 결과가 있으므로 사용자 영향이 작다 |

### 설계 수정 필요
| 대상 문서 | 내용 |
|-----------|------|
| [coupon-issue.md](../../design/05-features/coupon-issue.md) 3장 | 저장 컨슈머가 **저장 전 Redis `SISMEMBER` 로 보상된 발급을 걸러낸다**는 단계 추가. 쿠폰은 추첨의 건수 비교 같은 안전장치가 없어, 이 확인이 없으면 유령 이벤트가 그대로 **초과 발급**으로 이어진다 |
| [coupon-issue.md](../../design/05-features/coupon-issue.md) API 표 | 재초기화 API `POST /api/coupons/events/{eventId}/initialize` (ADMIN) 추가 (본문에는 언급되어 있으나 API 표에 없음) |
| [coupon-issue.md](../../design/05-features/coupon-issue.md) 발급 Lua 표 | 결과 `NOT_FOUND`(Redis meta 없음) 추가 |
| [architecture.md](../../design/02-architecture/architecture.md) 6장 | 인증 제외 경로에 `GET /api/coupons/events/*` 추가 (쿠폰 이벤트 조회는 공개) |
| [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.4 | `CouponGate` 메서드(`initialize`, `isIssued`, `remaining`), `CouponSaveService` 의 `CouponGate` 의존 추가 |

### 리스크
- **대기열이 없어 피크 요청이 전부 Redis 로**: 응모와 달리 유입 조절이 없다. 같은 Redis 를 쓰는 응모·대기열·랭킹이 쿠폰 오픈 순간 영향을 받을 수 있다 → 8단계에서 쿠폰 스파이크와 드로우 응모를 동시에 걸어 측정.
- **Redis 초기화 실패 후 발급 시작**: 등록 시 초기화가 실패하면 발급 요청이 `EVENT_NOT_FOUND` 로 거부된다. 오버 발급 방향이 아니라 서비스 불가 방향이므로 정합성은 안전. 재초기화 API 로 복구.
- **Redis 유실**: AOF 없이 재시작되면 잔여 수량·발급 Set 이 사라져 재초기화 시 초과 발급 가능. 0단계 AOF 설정이 전제.
- **Kafka 응답 지연**: 발급 응답 시간에 Kafka ack 왕복이 포함된다. 동시성 테스트(T6-13)에서 소요 시간을 기록해 8단계 비교 기준으로 쓴다.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 테스트 ID |
|----------------|-----------|
| 동시성: 수량 100, 1,000명 동시 요청 → 발급 정확히 100 | T6-13 |
| 동시성: 같은 사용자 10번 동시 요청 → 1매 | T6-14 |
| 발급 시작 전 요청 → `NOT_STARTED` | T6-04, T6-08 |
| 발행 실패 → 보상 후 원상복구 | T6-05, T6-09, T6-15 |
| (추가) 보상된 발급이 DB 에 남지 않음 (초과 발급 없음) | T6-06, T6-16 |
| (추가) 컨슈머 재전달 멱등 | T6-11, T6-17 |
| (추가) 재초기화가 진행 중 수량을 덮어쓰지 않음 | T6-10 |
| (추가) API 4개 + 재초기화 API | T6-18 ~ T6-20 |
| 공통: `./gradlew build` 통과 (ArchUnit 포함) | T6-21 |
| 공통: 설계 수정 필요 항목 반영 + 변경 이력 | 커밋 #7 |
| 공통: D6-1 ~ D6-3 결정 및 문서 반영 | 커밋 #7 |
