# 6단계 — 선착순 쿠폰 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 발행 정책을 4단계와 통일(애플리케이션 재시도 제거, producer 내부 재시도 + delivery.timeout 3000, get 3.5초), 저장 컨슈머 DLT(3회 재시도 후 `coupon.issued.DLT`) 추가, 저장 전 Redis 확인 확정, 재발급 유령 이벤트 테스트 추가, 설계 반영 완료 표시 |
| v1.2 | 2026-11-17 | DLT recoverer 에 목적지 리졸버 명시 (Spring Kafka 4 기본 `{topic}-dlt`, 4단계 v1.4 에서 확인) |
| v1.3 | 2026-11-17 | 결정 필요 D6-4·리스크에 메모 추가: 4단계 E-40/E-41(유령 이벤트가 보상보다 먼저 소비, 판정 Lua 타임아웃) 과 같은 방식 적용 필요. 설계 확정은 6단계 착수 시 |
| v1.4 | 2026-11-17 | 착수 전 정리. D6-4 확정 — 4단계 E-40/E-41 사용자 결정을 그대로 적용 (요청 ID `requests`, 저장 확정 `persisted` + claim Lua, 보상 3결과, Redis 예외 → `GateUnavailableException` → 보상 후 503, ALREADY_PERSISTED 면 202). D6-1~3 확정(기본안). 4단계 계약 정렬: `ApplicationException(CouponErrorCode)`·코드명 `COUPON_*`, 포트 id 는 `long`, `isIssued` 제거 → `claimForSave`. Redis 명령 타임아웃 1s (E-03). DLT 재발행 전용 producer `max.block.ms` 5s (E-50, 쿠폰부터). 테스트 T6-16·T6-22 재작성, T6-24~T6-30 추가. 신민아 착수 전 검토 반영: Requester+AdminGuard, CouponErrorCode.from, 등록 시 remaining 재조회 제거, 발행 어댑터 RuntimeException 감싸기, DLT JSON·bytes 전용 템플릿, GlobalExceptionHandler 확인, 리스너 pause, CLIENT PAUSE 타임아웃, gateway 변경 없음 |
| v1.5 | 2026-11-17 | Redis 장애 변환 위치 명시: `reinitialize`·`event` 는 서비스에서 `GateUnavailableException` → `ApplicationException(TEMPORARILY_FAILED)` (신민아 MAJOR — 그대로 올리면 500). `CouponQueryService.event` 는 트랜잭션 없이 |
| v1.6 | 2026-11-17 | #4 검토 반영: DLT 적재 ERROR 로그를 recoverer 래핑으로 (#6, T6-23 단언), 내 쿠폰 정렬에 id 보조 정렬 (#5), Mapper 2개로 파일 목록 정정 |
| v1.7 | 2026-11-17 | 리스너 id `coupon-saver`(idIsGroup=false, T6-16·T6-22 pause 용), T6-23 저장 실패 주입은 DB 트리거 (저장소 포트 대체 빈 대신) |

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
│   ├── exception
│   │   └── DomainException.java                 [신규] (entry 와 같은 형태)
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
│   │   ├── CouponQueryService.java              [신규]
│   │   └── AdminGuard.java                      [신규] package-private, Requester.isAdmin
│   ├── dto
│   │   ├── Requester.java                       [신규]
│   │   ├── CouponReinitializeCommand.java       [신규]
│   │   ├── CouponEventRegisterCommand.java      [신규]
│   │   ├── CouponIssueCommand.java              [신규]
│   │   ├── CouponSaveCommand.java               [신규]
│   │   ├── CouponEventResult.java               [신규]
│   │   ├── CouponIssueResult.java               [신규]
│   │   └── MyCouponResult.java                  [신규]
│   ├── exception
│   │   └── CouponErrorCode.java                 [신규] ApplicationException 과 함께 사용 (0단계 계약)
│   └── port
│       ├── CouponGate.java                      [신규]
│       ├── GateResult.java                      [신규]
│       ├── CouponIssuedPayload.java             [신규]
│       ├── CouponEventPublisher.java            [신규]
│       ├── CompensationResult.java              [신규] COMPENSATED / NOT_OURS / ALREADY_PERSISTED
│       ├── GateUnavailableException.java        [신규] Redis 장애(타임아웃 포함)
│       └── EventPublishException.java           [신규]
├── infrastructure
│   ├── persistence
│   │   ├── CouponEventJpaEntity.java            [신규]
│   │   ├── IssuedCouponJpaEntity.java           [신규]
│   │   ├── CouponEventJpaRepository.java        [신규]
│   │   ├── IssuedCouponJpaRepository.java       [신규]
│   │   ├── CouponEventRepositoryImpl.java       [신규]
│   │   ├── IssuedCouponRepositoryImpl.java      [신규]
│   │   ├── CouponEventMapper.java               [신규]
│   │   └── IssuedCouponMapper.java              [신규] (엔티티별 Mapper, entry 와 같은 형태)
│   ├── redis
│   │   ├── CouponKeys.java                      [신규]
│   │   └── RedisCouponGate.java                 [신규]
│   ├── messaging
│   │   └── KafkaCouponEventPublisher.java       [신규]
│   └── config
│       ├── ClockConfig.java                     [신규]
│       ├── RedisScriptConfig.java               [신규] Lua 3개 빈 (entry 와 같은 방식)
│       └── KafkaConsumerConfig.java             [신규] DefaultErrorHandler + DLT (4단계와 같은 패턴)
└── presentation
    ├── api
    │   ├── GlobalExceptionHandler.java          [확인·보강] entry 와 같은 처리
    │   ├── CouponEventController.java           [신규]
    │   ├── CouponController.java                [신규]
    │   ├── request/CouponEventRegisterRequest.java
    │   └── response/{CouponEventResponse, CouponIssueResponse, MyCouponResponse}.java
    └── listener
        └── CouponIssuedListener.java            [신규]

coupon-service/src/main/resources/
├── scripts/coupon-issue.lua                     [신규]
├── scripts/coupon-compensate.lua                [신규]
├── scripts/coupon-claim.lua                     [신규] 저장 확정 (E-40)
├── db/migration/V1__create_coupon_events.sql    [신규]
├── db/migration/V2__create_issued_coupons.sql   [신규]
└── application.yml                              [수정] Redis timeout 1s, listener ack-mode, coupon.* 설정

gateway/src/main/resources/application.yml       변경 없음 — 인증 제외 경로 `GET /api/coupons/events/{eventId}` 가 이미 있음
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
        if (issueStartAt == null) throw new DomainException("issueStartAt is required");
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
    Optional<CouponEvent> findById(long id);      // 포트 id 는 primitive long (4·5단계와 같음)
}

public interface IssuedCouponRepository {
    /** UK(coupon_event_id, user_id) 충돌 시 저장하지 않고 false */
    boolean saveIfAbsent(IssuedCoupon coupon);
    List<IssuedCoupon> findByUserId(long userId);
}
```

---

### 3.2 application

#### 포트
```java
package com.queuedrop.coupon.application.port;

/**
 * Redis 기반 선착순 발급 게이트. tryIssue·compensate·claimForSave 는 Redis 장애(명령 타임아웃 포함) 시
 * GateUnavailableException 을 던진다 (4단계 EntryGate 와 같은 계약, E-41).
 * couponEventId = 쿠폰 이벤트 ID, requestId = 발급 요청 ID(= coupon.issued 메시지의 eventId).
 */
public interface CouponGate {
    void initialize(long couponEventId, int totalQuantity, Instant issueStartAt);   // SET NX, 멱등
    /** OK 면 requests[userId] = requestId 를 함께 기록한다. requestId 는 호출자가 판정 전에 만든다 (E-40) */
    GateResult tryIssue(long couponEventId, long userId, String requestId, Instant now);
    /** @return ALREADY_PERSISTED(저장 확정됨, 되돌리지 않음) / NOT_OURS(이 요청의 발급 아님) / COMPENSATED */
    CompensationResult compensate(long couponEventId, long userId, String requestId);
    /** 저장 컨슈머가 DB 저장 전에 호출. false 면 유령 이벤트 — 저장하지 않는다. 재전달에도 true (멱등) */
    boolean claimForSave(long couponEventId, long userId, String requestId);
    long remaining(long couponEventId);
}

public interface CouponEventPublisher {
    void publishIssued(CouponIssuedPayload payload);    // 브로커 ack 까지 동기 대기, 실패 시 예외
}
```

#### DTO
```java
public record Requester(Long userId, String role) { public boolean isAdmin() { return "ADMIN".equals(role); } }   // product·entry 와 같은 형태
public record CouponEventRegisterCommand(Requester requester, String name, int totalQuantity, Instant issueStartAt) {}
public record CouponReinitializeCommand(Requester requester, Long couponEventId) {}
public record CouponIssueCommand(Long couponEventId, Long userId) {}
public record CouponSaveCommand(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}   // eventId = 요청 ID

// application/port 에 둔다 (entry 와 같은 위치): GateResult(IssueOutcome outcome), CouponIssuedPayload(String eventId, Long couponEventId, Long userId, Instant issuedAt)

public record CouponEventResult(Long id, String name, int totalQuantity, long remaining, Instant issueStartAt) {}
public record CouponIssueResult(Long couponEventId, Instant issuedAt) {}
public record MyCouponResult(Long couponEventId, String eventName, Instant issuedAt) {}
```
- `GateResult`·`CouponIssuedPayload` 는 entry 와 같이 `application/port` 에 둔다. `GateResult` 는 domain enum 을 담지만 **포트 반환 타입**이라 application 안에서만 쓰인다. 서비스 public 메서드는 `CouponIssueResult` 만 반환한다 (ArchUnit R6).
- `MyCouponResult.eventName` 을 위해 조회 시 `coupon_events` 를 함께 읽는다 (같은 서비스 DB).

#### 에러 코드
```java
/** EntryErrorCode 와 같은 형태. 던질 때는 new ApplicationException(CouponErrorCode.X) (0단계 계약) */
public enum CouponErrorCode implements ErrorCode {
    EVENT_NOT_FOUND("COUPON_EVENT_NOT_FOUND", 404, "쿠폰 이벤트를 찾을 수 없습니다."),
    NOT_STARTED("COUPON_NOT_STARTED", 409, "아직 발급 시작 전입니다."),
    DUPLICATE("COUPON_DUPLICATE", 409, "이미 발급받은 쿠폰입니다."),
    SOLD_OUT("COUPON_SOLD_OUT", 409, "쿠폰이 모두 소진되었습니다."),
    TEMPORARILY_FAILED("COUPON_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요."),
    INVALID_EVENT("COUPON_INVALID_EVENT", 400, "쿠폰 이벤트 정보가 올바르지 않습니다.");
    // code/status/message 필드·접근자는 EntryErrorCode 와 동일

    /** domain 판정 결과 → 에러 코드. 변환은 여기 한 곳에서만 한다 */
    public static CouponErrorCode from(IssueOutcome outcome) {
        return switch (outcome) {
            case NOT_FOUND -> EVENT_NOT_FOUND;        // 404 (Redis meta 없음 = 미등록 또는 초기화 실패)
            case NOT_STARTED -> NOT_STARTED;
            case DUPLICATE -> DUPLICATE;
            case SOLD_OUT -> SOLD_OUT;
            case OK -> throw new IllegalArgumentException("OK 는 에러가 아닙니다");
        };
    }
}
```
- 관리자 권한 없음은 공통 `CommonErrorCode.FORBIDDEN` — `application/service/AdminGuard`(package-private, `requireAdmin(Requester)`)를 product·entry 와 같은 형태로 둔다.
- `CouponException` 은 쓰지 않는다 — `ApplicationException(CouponErrorCode, ...)` 로 통일 (v1.4).

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
        AdminGuard.requireAdmin(command.requester());
        CouponEvent event;
        try {
            event = CouponEvent.create(command.name(), command.totalQuantity(), command.issueStartAt(), clock.instant());
        } catch (DomainException e) {
            throw new ApplicationException(CouponErrorCode.INVALID_EVENT, e);
        }
        CouponEvent saved = eventRepository.save(event);          // 커밋 완료
        try {
            gate.initialize(saved.getId(), saved.getTotalQuantity(), saved.getIssueStartAt());
        } catch (GateUnavailableException e) {
            log.warn("coupon redis init failed couponEventId={} — reinitialize required", saved.getId(), e);
        }
        // remaining 을 Redis 에서 다시 읽지 않는다: 초기화 실패(Redis 장애) 중이면 이 호출도 실패해 500 이 된다.
        // 막 등록한 이벤트의 잔여는 totalQuantity 다 (초기화 실패 시에도 응답은 등록 성공 — 재초기화로 복구)
        return toResult(saved, saved.getTotalQuantity());
    }

    /** 관리자 재초기화: SET NX 라 진행 중인 잔여 수량을 덮어쓰지 않는다.
     *  Redis 장애(GateUnavailableException)는 서비스에서 ApplicationException(TEMPORARILY_FAILED) 로 감싸 503 (GlobalExceptionHandler 는 ApplicationException 만 처리) */
    public void reinitialize(CouponReinitializeCommand command) {
        AdminGuard.requireAdmin(command.requester());
        CouponEvent event = eventRepository.findById(command.couponEventId())
                .orElseThrow(() -> new ApplicationException(CouponErrorCode.EVENT_NOT_FOUND));
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

    /** 4단계 EntryApplyService.apply 와 같은 흐름 (E-40·E-41) */
    public CouponIssueResult issue(CouponIssueCommand command) {
        Instant now = clock.instant();
        String requestId = UUID.randomUUID().toString();        // 요청 ID = 메시지 eventId. 판정 전에 만든다 (E-40)

        GateResult result;
        try {
            result = gate.tryIssue(command.couponEventId(), command.userId(), requestId, now);
        } catch (GateUnavailableException e) {
            // Lua 실행 여부를 모름 (E-41): 실행됐다면 되돌리고(COMPENSATED), 아니면 NOT_OURS 로 무변화
            compensateQuietly(command, requestId);
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
        if (result.outcome() != IssueOutcome.OK) {
            throw new ApplicationException(CouponErrorCode.from(result.outcome()));
        }

        CouponIssuedPayload payload = new CouponIssuedPayload(requestId, command.couponEventId(), command.userId(), now);
        try {
            publisher.publishIssued(payload);                   // ack 까지 대기 (재시도는 producer 내부, 최대 3초)
        } catch (EventPublishException e) {
            Optional<CompensationResult> r = compensateQuietly(command, requestId);
            if (r.filter(x -> x == CompensationResult.ALREADY_PERSISTED).isPresent()) {
                // 컨슈머가 이미 저장 확정 = 실제로 발급됨 → 성공 (불변식 "성공 = 기록")
                log.warn("발행 실패로 판단했으나 이미 저장됨 couponEventId={} userId={} requestId={}", ...);
                return new CouponIssueResult(command.couponEventId(), now);
            }
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
        return new CouponIssueResult(command.couponEventId(), now);
    }

    /** 보상 자체가 실패하면 empty (ERROR 로그, 호출자는 503). 수량 한 자리가 새는 쪽(언더 발급)이라 초과 발급은 아니다 */
    private Optional<CompensationResult> compensateQuietly(CouponIssueCommand command, String requestId) { /* EntryApplyService 와 동일 */ }
}
```
- `publishIssued` 는 `send()` 가 바로 던지는 예외(`max.block.ms` 초과 등)도 `EventPublishException` 으로 감싼다 (4단계 어댑터와 동일).
- 의존 그래프에 `IssuedCouponRepository` 가 없다 — 응답 경로에서 DB 를 뺀 설계가 그대로 드러난다.

#### `CouponSaveService` — 저장 (컨슈머 경로)
```java
@Service
@RequiredArgsConstructor
public class CouponSaveService {
    private final IssuedCouponRepository issuedRepository;
    private final CouponGate gate;

    public void save(CouponSaveCommand command) {
        // ① 저장 확정 (claim Lua): 보상된 발급 또는 이전 시도의 유령 이벤트면 false. true 이후 보상은 거절된다 (E-40)
        //    없으면 보상으로 되돌아간 수량을 다른 사람이 받아 초과 발급이 된다
        if (!gate.claimForSave(command.couponEventId(), command.userId(), command.eventId())) {
            log.warn("유령 발급 이벤트 무시 couponEventId={} userId={} requestId={}",
                    command.couponEventId(), command.userId(), command.eventId());
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
> 응모(4단계)는 유령 이벤트가 저장되면 "응모 수 불일치" 로 드러나지만, 쿠폰은 건수 비교 단계가 없어 **조용히 초과 발급**이 된다. 그래서 저장 전 Redis 확인이 쿠폰에서는 필수다 (**확정, 2026-11-16**).
> - claim 은 "보상이 먼저 / 소비가 먼저" 두 순서를 모두 막는다: 소비가 먼저면 claim 이 `persisted` 에 넣어 이후 보상이 ALREADY_PERSISTED 로 거절되고(발급 성공 202), 보상이 먼저면 `requests` 가 지워져 claim 이 false (4단계 D2 v1.3).
> - DB 장애 등으로 저장이 실패하면 예외를 그대로 올린다 → `DefaultErrorHandler` 가 1초 간격 3회 재시도 후 `coupon.issued.DLT` 로 보낸다 (4단계와 같은 정책).

#### `CouponQueryService`
```java
@Service
@RequiredArgsConstructor
public class CouponQueryService {
    // event 는 트랜잭션을 열지 않는다: DB 커넥션을 쥔 채 Redis 를 호출하지 않기 위해 (readOnly 는 myCoupons 에만)
    public CouponEventResult event(long couponEventId) { /* DB 이벤트(없으면 404) + gate.remaining (GateUnavailableException 은 서비스에서 TEMPORARILY_FAILED 로 감쌈 → 503) */ }
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
| `coupon:{eventId}:requests` | Hash | userId → 발급 자리를 잡은 요청 ID (E-40) |
| `coupon:{eventId}:persisted` | Set | 저장 확정된 userId (claim Lua, E-40) |

> 키의 `{eventId}` 는 쿠폰 이벤트 ID(couponEventId)다. 다섯 키 모두 같은 ID 를 쓴다. Redis Cluster 전환 시에는 `coupon:{eventId}` 의 `{}` 해시태그로 같은 슬롯에 배치되므로 Lua 다중 키 실행이 가능하다.

#### `scripts/coupon-issue.lua`
```lua
-- KEYS[1] = coupon:{eventId}:meta
-- KEYS[2] = coupon:{eventId}:remaining
-- KEYS[3] = coupon:{eventId}:issued
-- KEYS[4] = coupon:{eventId}:requests
-- ARGV[1] = userId
-- ARGV[2] = nowEpochMs (애플리케이션 시각, 비결정적 TIME 명령 사용 금지)
-- ARGV[3] = requestId (요청 ID, v1.4)

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
redis.call('HSET', KEYS[4], ARGV[1], ARGV[3])   -- 이 자리를 잡은 요청 기록 (보상·저장 확정이 대조)
return 'OK'
```

#### `scripts/coupon-compensate.lua` (4단계 entry-compensate.lua 와 같은 구조)
```lua
-- KEYS[1] = coupon:{eventId}:remaining
-- KEYS[2] = coupon:{eventId}:issued
-- KEYS[3] = coupon:{eventId}:requests
-- KEYS[4] = coupon:{eventId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = requestId (되돌릴 요청)
-- return  : 1 = COMPENSATED, 0 = NOT_OURS, 2 = ALREADY_PERSISTED
if redis.call('SISMEMBER', KEYS[4], ARGV[1]) == 1 then
  return 2                                         -- 저장 확정됨: 되돌리지 않는다
end
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0                                         -- 이 요청의 발급이 아님 (판정 미실행, 이미 보상, 다른 요청)
end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('INCR', KEYS[1])
return 1
```
- 두 번 호출해도 두 번째는 `requests` 가 지워져 NOT_OURS → 수량이 늘지 않는다.

#### `scripts/coupon-claim.lua` (4단계 entry-claim.lua 와 같은 구조)
```lua
-- KEYS[1] = coupon:{eventId}:issued
-- KEYS[2] = coupon:{eventId}:requests
-- KEYS[3] = coupon:{eventId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = requestId (메시지의 eventId)
-- return  : 1 = 저장 확정 (재전달에도 1), 0 = 저장하지 않음 (유령 이벤트)
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return 0                                         -- 보상된 발급
end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0                                         -- 보상 후 재발급한 사용자의 이전 시도 유령 이벤트
end
redis.call('SADD', KEYS[3], ARGV[1])
return 1
```

#### `RedisCouponGate`
```java
@Component
@RequiredArgsConstructor
public class RedisCouponGate implements CouponGate {
    private final StringRedisTemplate redis;
    // 빈 이름 = 필드 이름 (infrastructure/config/RedisScriptConfig, entry 와 같은 방식)
    private final RedisScript<String> couponIssueScript;        // scripts/coupon-issue.lua (EVALSHA)
    private final RedisScript<Long> couponCompensateScript;     // scripts/coupon-compensate.lua
    private final RedisScript<Long> couponClaimScript;          // scripts/coupon-claim.lua

    @Override
    public void initialize(long couponEventId, int totalQuantity, Instant issueStartAt) {
        call(() -> {
            redis.opsForValue().setIfAbsent(CouponKeys.remaining(couponEventId), String.valueOf(totalQuantity));   // SET NX
            redis.opsForHash().putIfAbsent(CouponKeys.meta(couponEventId), "issueStartAt",
                    String.valueOf(issueStartAt.toEpochMilli()));                                                  // HSETNX
            return null;
        });
    }

    // tryIssue / compensate / claimForSave: 위 Lua 세 개를 실행. 모든 Redis 호출은 call() 로 감싸
    // DataAccessException(연결 실패·QueryTimeoutException 등) → GateUnavailableException (4단계 RedisEntryGate 와 동일)
    @Override public long remaining(long couponEventId) { /* call(GET), 없으면 0 */ }
}
```
- 명령 타임아웃: `spring.data.redis.timeout: 1s` (Lettuce 기본 60초, E-03 — entry 와 같음).

#### `KafkaCouponEventPublisher`
```java
@Component
class KafkaCouponEventPublisher implements CouponEventPublisher {

    private final KafkaTemplate<String, Object> kafka;
    private final Duration awaitTimeout;

    KafkaCouponEventPublisher(KafkaTemplate<String, Object> kafka,
                              @Value("${coupon.publish.await-timeout}") Duration awaitTimeout) {
        this.kafka = kafka;
        this.awaitTimeout = awaitTimeout;
    }

    @Override
    public void publishIssued(CouponIssuedPayload p) {
        CouponIssued message = new CouponIssued(p.eventId(), p.couponEventId(), p.userId(), p.issuedAt());
        String key = p.couponEventId() + ":" + p.userId();
        try {
            kafka.send(Topics.COUPON_ISSUED, key, message)
                 .get(awaitTimeout.toMillis(), TimeUnit.MILLISECONDS);   // 애플리케이션 재시도 없음
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("발행 대기 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("coupon.issued 발행 실패", e);
        } catch (RuntimeException e) {          // send() 가 동기로 던지는 경우 (직렬화 실패, max.block.ms 초과 등)
            throw new EventPublishException("coupon.issued 발행 실패", e);   // 보상 경로로 보낸다 (없으면 500 + Redis 에만 발급)
        }
    }
}
```
- 파티션 키 `couponEventId:userId` ([architecture.md](../../design/02-architecture/architecture.md) 4장).
- **발행 정책 (확정, 4단계와 동일)**: 재시도는 producer 내부(멱등)에만 맡기고 애플리케이션은 다시 send 하지 않는다. 같은 메시지를 다시 send 하면 멱등 producer 도 별개 레코드로 취급해 유령 이벤트가 늘어나고 응답이 최대 9초까지 길어진다 ([phase-4-entry-apply.md](phase-4-entry-apply.md) 3장 "Producer 설정 근거").
- producer·consumer 설정 (`application.yml`, 4단계와 동일):
```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
        linger.ms: 0
        request.timeout.ms: 1000
        delivery.timeout.ms: 3000
    consumer:
      group-id: ${spring.application.name}
      auto-offset-reset: earliest
      enable-auto-commit: false
    listener:
      ack-mode: record
      concurrency: 3              # coupon.issued 파티션 6 → 인스턴스 2개 기준 3씩 (entry 와 같음)
  data:
    redis:
      timeout: 1s                 # 명령 타임아웃 (E-03)

coupon:
  publish:
    await-timeout: 3500ms         # delivery.timeout.ms(3000) + 여유
```

#### `KafkaConsumerConfig` — 저장 실패 시 DLT (4단계와 같은 패턴)
```java
/** entry KafkaConsumerConfig 와 같은 형태 + DLT 전용 producer (E-50) */
@Configuration
public class KafkaConsumerConfig implements DisposableBean {

    static final long DLT_MAX_BLOCK_MS = 5_000L;   // 공통 producer 의 1초를 물려받지 않는다 (E-50)

    /** 둘 다 빈이 아니다 (KafkaTemplate·ProducerFactory 빈을 더 두면 Boot 자동 구성 빈이 빠진다) → destroy() 에서 닫는다 */
    private DefaultKafkaProducerFactory<String, Object> dltJsonFactory;
    private DefaultKafkaProducerFactory<String, byte[]> dltBytesFactory;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<String, Object> producerFactory) {
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, DLT_MAX_BLOCK_MS);
        dltJsonFactory = new DefaultKafkaProducerFactory<>(config);                                  // 기존 직렬화 설정 그대로
        dltBytesFactory = new DefaultKafkaProducerFactory<>(config, new StringSerializer(), new ByteArraySerializer());

        // 값 타입 순서 매칭: 역직렬화 실패 레코드(byte[])는 원본 바이트 그대로 (T6-29), 나머지는 JSON
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, new KafkaTemplate<>(dltBytesFactory));
        templates.put(Object.class, new KafkaTemplate<>(dltJsonFactory));

        // Spring Kafka 4 기본 목적지는 {topic}-dlt — 리졸버로 coupon.issued.DLT·같은 파티션 지정
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
        // DeserializationException 은 기본으로 재시도하지 않는 예외 (addNotRetryableExceptions 불필요)
    }

    @Override
    public void destroy() { /* dltJsonFactory·dltBytesFactory destroy (null 검사) */ }
}
```
- DLT 토픽 `coupon.issued.DLT` 는 0단계 토픽 초기화에서 만든다.
- DLT 재발행 producer 를 JSON·bytes 둘 다 전용 ProducerFactory 로 만들어 `max.block.ms: 5000` 을 준다 (E-50). 공통 producer 의 1초를 물려받으면 첫 DLT 전송이 메타데이터 대기로 실패해 재처리 루프에 들어갈 수 있다. entry·product 는 E-50 에 남긴다.
- **DLT 적재 ERROR 로그 (v1.6)**: recoverer 를 감싸 DLT 로 보낼 때 `log.error(topic, partition, offset, key, 원인)` 를 남긴 뒤 위임한다 — Spring Kafka 가 보장하지 않으며, Redis 에만 남은 발급을 감지할 수단이 이 로그뿐이다. 커밋 #6 에서 T6-23 의 ERROR 로그 단언과 함께 반영 (신민아 #4 MAJOR).
- DLT 로 간 발급은 Redis 에는 있고 DB 에는 없는 상태다. 응모와 달리 이를 자동으로 드러내는 단계가 없으므로 **DLT 적재를 ERROR 로그로 남기고 재처리**한다 (리스크 참고).

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
interface IssuedCouponJpaRepository extends JpaRepository<IssuedCouponJpaEntity, Long> {

    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO issued_coupons (coupon_event_id, user_id, issued_at)
        VALUES (:eventId, :userId, :issuedAt)
        ON CONFLICT (coupon_event_id, user_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") Long eventId, @Param("userId") Long userId, @Param("issuedAt") Instant issuedAt);

    List<IssuedCouponJpaEntity> findByUserIdOrderByIssuedAtDescIdDesc(Long userId);   // 같은 시각이면 id 역순 (T6-20 결정성, #5)
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
    public ResponseEntity<CouponEventResponse> register(@RequestHeader("X-User-Id") Long userId, @RequestHeader("X-User-Role") String role,
                                                        @Valid @RequestBody CouponEventRegisterRequest request) {
        CouponEventResult result = eventService.register(request.toCommand(new Requester(userId, role)));
        return ResponseEntity.status(HttpStatus.CREATED).body(CouponEventResponse.from(result));
    }

    @GetMapping("/{eventId:\\d+}")
    public CouponEventResponse get(@PathVariable Long eventId) {
        return CouponEventResponse.from(queryService.event(eventId));
    }

    @PostMapping("/{eventId:\\d+}/initialize")
    public ResponseEntity<Void> reinitialize(@RequestHeader("X-User-Id") Long userId, @RequestHeader("X-User-Role") String role,
                                             @PathVariable Long eventId) {
        eventService.reinitialize(new CouponReinitializeCommand(new Requester(userId, role), eventId));
        return ResponseEntity.noContent().build();
    }
}

@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {
    private final CouponIssueService issueService;
    private final CouponQueryService queryService;

    @PostMapping("/events/{eventId:\\d+}/issue")
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
- 에러 응답: coupon 의 `presentation/api/GlobalExceptionHandler.java`(0단계 틀, 서비스별 presentation 클래스)가 entry 의 것과 같은 처리(`ApplicationException`, 헤더 누락 400 `COMMON_MISSING_USER_HEADER`, 검증 400)를 갖추는지 확인·보강한다. 경로 `{eventId:\d+}` 는 entry 와 같은 패턴 (숫자 아니면 404).
- `/me` 는 DB 기준이라 발급 직후(컨슈머 처리 전)에는 비어 있을 수 있다. 응모처럼 Redis 로 보완하지 않는 이유: 쿠폰 목록은 이벤트 이름·발급 시각이 필요한데 Redis Set 에는 userId 만 있다 (구현 결정, 6장 리스크).

#### `CouponIssuedListener`
```java
@Component
@RequiredArgsConstructor
public class CouponIssuedListener {
    private final CouponSaveService saveService;

    // id 는 테스트의 pause/resume 용 (T6-16·T6-22). idIsGroup=false 라 groupId 는 공통 설정(${spring.application.name})
    @KafkaListener(id = "coupon-saver", idIsGroup = false, topics = Topics.COUPON_ISSUED)
    public void on(CouponIssued message) {
        saveService.save(new CouponSaveCommand(message.eventId(), message.couponEventId(), message.userId(), message.issuedAt()));
    }
}
```

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ/NFR |
|----|-----------|---------------|-------------------------------|---------------|
| T6-01 | domain 단위 | `CouponEventTest` | G: 수량 0 / 빈 이름 / 시작 시각 과거·null / W: `create` / T: 각각 `DomainException` | REQ-COUPON-01 |
| T6-02 | domain 단위 | `CouponEventTest` | G: 시작 12:00 / W: `isStarted(11:59:59)`, `(12:00)` / T: false, true | REQ-COUPON-02 |
| T6-03 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Fake Gate OK, Fake publisher 성공 / W: `issue` / T: 결과 반환, 발행 1건 | REQ-COUPON-02 |
| T6-04 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Gate 가 NOT_STARTED / DUPLICATE / SOLD_OUT / NOT_FOUND / W: `issue` / T: 각 에러 코드 예외, 발행 없음 | REQ-COUPON-02, 03 |
| T6-05 | application 단위 (Fake) | `CouponIssueServiceTest` | G: Fake publisher 예외 + 보상 결과 COMPENSATED / NOT_OURS / 보상 자체 예외 / W: `issue` / T: 셋 다 `TEMPORARILY_FAILED`, Gate 에 같은 requestId 로 compensate 1회, 보상 예외면 ERROR 로그 | NFR-03 |
| T6-06 | application 단위 (Fake) | `CouponSaveServiceTest` | G: claimForSave false (보상됨 / 다른 requestId) / W: `save` / T: 저장 안 함, WARN | NFR-01, 03 |
| T6-07 | application 단위 (Fake) | `CouponEventServiceTest` | G: USER 권한 / W: `register`·`reinitialize` / T: `FORBIDDEN`(조회 전). G: Gate 초기화 GateUnavailableException (remaining 도 실패하도록) / W: `register` / T: DB 저장됨, 결과 반환(remaining = totalQuantity), WARN 로그 | REQ-COUPON-01 |
| T6-08 | infra 통합 (Redis) | `RedisCouponGateTest` | Lua 결과 5종: 미초기화 → NOT_FOUND, 시작 전 → NOT_STARTED, 첫 발급 → OK(`requests[user] = requestId` 기록), 재발급 → DUPLICATE, 수량 소진 → SOLD_OUT. 거절 결과는 remaining·issued·requests 변화 없음 | REQ-COUPON-02, 03 |
| T6-09 | infra 통합 (Redis) | `RedisCouponGateTest` | G: 발급 1건 / W: `compensate` 2번 / T: COMPENSATED 후 NOT_OURS, 잔여 1만 복구, Set·requests 제거. 다른 requestId 로 보상 → NOT_OURS 무변화. 판정 없이 compensate → NOT_OURS | NFR-03 |
| T6-10 | infra 통합 (Redis) | `RedisCouponGateTest` | G: 수량 100 초기화 후 30개 발급 / W: `initialize` 재호출 / T: 잔여 70 유지 (SET NX) | REQ-COUPON-01 |
| T6-11 | infra 통합 (PostgreSQL) | `IssuedCouponRepositoryImplTest` | W: 같은 (event, user) 2번 `saveIfAbsent` / T: true, false, 행 1개 | NFR-02 |
| T6-12 | infra 통합 (Kafka) | `KafkaCouponEventPublisherTest` | W: `publishIssued` / T: `coupon.issued` 토픽에 키 `eventId:userId` 메시지 1건 | - |
| T6-13 | **동시성 (전체 컨텍스트)** | `CouponIssueConcurrencyTest` | G: 수량 100, 서로 다른 사용자 1,000명 / W: 32스레드로 동시 `issue` / T: 성공 정확히 100, SOLD_OUT 900, Redis 잔여 0, Set 100, **최종 DB 100건** (Awaitility) | NFR-01 |
| T6-14 | **동시성 (전체 컨텍스트)** | `CouponIssueConcurrencyTest` | G: 수량 100, 같은 사용자 / W: 10스레드 동시 `issue` / T: 성공 1, DUPLICATE 9, DB 1건 | NFR-02 |
| T6-15 | 통합 (전체 컨텍스트) | `CouponIssueFailureTest` | G: 테스트용 `@Primary` publisher 가 항상 예외 / W: `issue` / T: 503, Redis 잔여·Set 원상복구 | NFR-03 |
| T6-16 | 통합 (전체 컨텍스트) | `CouponGhostEventTest` | (보상이 먼저) G: 수량 1. **`KafkaListenerEndpointRegistry` 로 저장 리스너 pause** → A 발급 → publisher 가 실제 발행 후 예외 → 보상 완료 → B 발급 성공 → resume (entry T4-31 `withSaverPaused` 와 같은 방식) / W: 컨슈머 처리 완료 / T: DB 에 B 1건만, A 없음 (**초과 발급 없음**), A 응답 503 | NFR-01, 03 |
| T6-17 | 통합 (전체 컨텍스트) | `CouponIssuedListenerTest` | G: Redis `issued` 에 user, `requests[user] = 메시지 eventId` 세팅 (없으면 claim false) 후 같은 `coupon.issued` 메시지 3번 발행 / W: 소비 / T: DB 1건 | NFR-02 |
| T6-18 | presentation 슬라이스 | `CouponControllerTest` | 에러 코드 → HTTP: NOT_STARTED/DUPLICATE/SOLD_OUT 409, EVENT_NOT_FOUND 404, TEMPORARILY_FAILED 503, 성공 202. X-User-Id 누락 → 400 `COMMON_MISSING_USER_HEADER`, 숫자 아닌 eventId → 404 | REQ-COUPON-02 |
| T6-19 | presentation 슬라이스 | `CouponEventControllerTest` | 요청 검증: 수량 0, 과거 시작 시각 → 400. 정상 → 201. `GET` 응답에 `remaining` 포함 | REQ-COUPON-01, 04 |
| T6-20 | 통합 (전체 컨텍스트) | `MyCouponQueryTest` | G: 이벤트 2개에서 각각 발급 / W: `GET /me` / T: 2건, 이벤트 이름 포함, 최신순 | REQ-COUPON-04 |
| T6-21 | 아키텍처 | `ArchitectureTest` | R1~R7 통과. `CouponIssueService` 가 `IssuedCouponRepository` 에 의존하지 않음 (추가 규칙, 구현 결정) | - |
| T6-22 | 통합 (전체 컨텍스트) | `CouponGhostEventTest` | G: 수량 2, 저장 리스너 pause. A 발급 → 실제 발행 후 예외 주입 → 보상 → **A 재발급 성공**(새 requestId) → B 발급 성공 / W: 컨슈머 처리 완료 / T: DB 에 A 1건(재발급 requestId), B 1건 (총 2 = 수량), Redis 잔여 0. 이전 시도 유령 이벤트는 claim false | NFR-01, 03 |
| T6-23 | 통합 (전체 컨텍스트) | `CouponIssuedListenerTest` | G: Redis `issued`·`requests` 세팅(claim true), 해당 user_id INSERT 를 거부하는 PostgreSQL 트리거로 저장 실패 주입 (저장소 포트는 domain 타입이라 presentation 테스트에서 대체 빈을 두지 않음) / W: `coupon.issued` 1건 소비 / T: 3회 재시도 후 `coupon.issued.DLT` 에 1건, DLT 적재 ERROR 로그 1줄(topic·offset·key) | NFR-03 |
| T6-24 | infra 통합 (Redis) | `RedisCouponGateTest` | claim: 발급 후 같은 requestId → true(재호출도 true) / 다른 requestId → false / 보상 후 → false. claim 후 보상 → ALREADY_PERSISTED, 수량·Set 유지 | NFR-01, 03 (E-40) |
| T6-25 | application 단위 (Fake) | `CouponIssueServiceTest` | G: 발행 예외 + 보상 결과 ALREADY_PERSISTED / W: `issue` / T: 성공 결과 반환(202), WARN | NFR-03 (E-40) |
| T6-26 | application 단위 (Fake) | `CouponIssueServiceTest` | G: tryIssue 가 GateUnavailableException — ① Lua 실행 후 타임아웃(Fake 가 상태 반영 후 예외 → 보상 COMPENSATED, 수량 원복) ② 실행 전 실패(NOT_OURS, 무변화) ③ 보상도 실패 / W: `issue` / T: 셋 다 같은 requestId 로 보상 시도 후 `TEMPORARILY_FAILED`, 발행 없음, ③ 은 ERROR 로그 | NFR-03 (E-41) |
| T6-27 | 통합 (전체 컨텍스트) | `CouponGhostEventTest` | (소비가 먼저) G: 수량 1. A 발급 → publisher 가 실제 발행 후 **컨슈머 저장 완료를 기다린 뒤** 예외 / W: 보상 → B 발급 / T: A 응답 202, DB A 1건, B SOLD_OUT, Redis 잔여 0 (**초과 발급 없음**) | NFR-01, 03 (E-40) |
| T6-28 | infra 통합 (Redis) | `RedisCouponGateTest` | G: 별도 연결로 `CLIENT PAUSE 1500 WRITE` (명령 타임아웃 1s 초과, entry T4-39 와 같은 방식) / W: tryIssue / T: `GateUnavailableException`, pause 해제 후 Lua 가 실행돼 있으면 같은 requestId 보상으로 원복. 잘못된 포트로 compensate·claimForSave·initialize·remaining 도 `GateUnavailableException` | NFR-03 (E-41) |
| T6-29 | 통합 (전체 컨텍스트) | `CouponIssuedListenerTest` | G: JSON 아닌 바이트 / W: 소비 / T: 재시도 없이 `coupon.issued.DLT` 에 원본 바이트, 다음 메시지 정상 처리 | NFR-03 |
| T6-30 | application 단위 | `CouponErrorCodeTest` | `from(IssueOutcome)` 4종 매핑(NOT_FOUND → EVENT_NOT_FOUND 404 등), OK → IllegalArgumentException, 코드명·HTTP 상태 | REQ-COUPON-02 |

---

## 5. 커밋 계획

| # | 범위 | 커밋 메시지 |
|---|------|-------------|
| 0 | docs | `docs: 6단계 착수 전 결정 반영`<br>- D6-1~D6-4 확정, 4단계 계약 정렬, 설계 문서·플랜·백로그 갱신 |
| 1 | domain | `feat(coupon): 쿠폰 도메인 모델 추가`<br>- CouponEvent(생성 검증, isStarted), IssuedCoupon, IssueOutcome<br>- Repository 포트 2개<br>- T6-01, T6-02 |
| 2 | application | `feat(coupon): 쿠폰 등록·발급·저장 유스케이스 구현`<br>- CouponEventService, CouponIssueService, CouponSaveService, CouponQueryService<br>- 포트 CouponGate, CouponEventPublisher, CompensationResult, GateUnavailableException, EventPublishException, 에러 코드<br>- T6-03 ~ T6-07, T6-25, T6-26, T6-30 (Fake 포트) |
| 3 | infra (Redis) | `feat(coupon): 선착순 발급 Lua 스크립트와 Gate 구현`<br>- coupon-issue.lua, coupon-compensate.lua, coupon-claim.lua, RedisCouponGate(SET NX 초기화, call() 예외 변환), Redis timeout 1s<br>- T6-08 ~ T6-10, T6-24, T6-28 |
| 4 | infra (DB, Kafka) | `feat(coupon): 쿠폰 영속성과 coupon.issued 발행 구현`<br>- V1, V2 마이그레이션, ON CONFLICT DO NOTHING 저장<br>- KafkaCouponEventPublisher (acks=all, producer 내부 재시도, delivery.timeout 3000)<br>- KafkaConsumerConfig (3회 재시도 후 coupon.issued.DLT, DLT 전용 producer max.block 5s)<br>- T6-11, T6-12 |
| 5 | presentation | `feat(coupon): 쿠폰 API와 발급 이벤트 리스너 추가`<br>- CouponEventController, CouponController, CouponIssuedListener<br>- T6-18 ~ T6-20 |
| 6 | 동시성·장애 테스트 | `test(coupon): 동시 발급·보상·유령 이벤트 테스트 추가`<br>- T6-13 ~ T6-17, T6-21 ~ T6-23, T6-27, T6-29 |
| 7 | docs | `docs: 6단계 구현 결과 반영`<br>- 설계 수정 필요 항목 반영, 플랜 진행 현황 갱신 |

---

## 6. 결정 필요 · 리스크

### 결정 사항 (확정, 2026-11-16)
| 항목 | 결정 |
|------|------|
| 발행 정책 | 4단계와 동일: 애플리케이션 재시도 없음, producer 내부 재시도 + `delivery.timeout.ms=3000`, `get` 대기 3.5초, 실패 시 보상 |
| 저장 전 Redis 확인 | 필수 — v1.4 부터 `gate.claimForSave`(claim Lua) 로 대체 (유령 이벤트로 인한 초과 발급 방지) |
| 저장 컨슈머 실패 | 3회 재시도 후 `coupon.issued.DLT` |

### 결정 필요
| ID | 항목 | 선택지 | 기본안 |
|----|------|--------|--------|
| D6-1 | 응모(4단계)와의 공통화 (Lua 실행·동기 발행·보상) | (a) **서비스별로 각자 구현** — 중복은 있지만 서비스 독립성 유지 (b) `common` 에 인프라 헬퍼(`LuaScriptRunner`, `SyncKafkaPublisher`) 추가 — common 이 Spring Data Redis·Spring Kafka 에 의존하게 됨 (c) 별도 `common-infra` 모듈 신설 — 순수 계약 모듈(common)과 분리, 모듈 1개 증가 | **✅ 확정(기본안, 2026-11-17): (a)**. 중복 대상이 두 서비스·클래스 2~3개 수준이라 추상화 비용이 더 크다. 8단계 이후 세 번째 사용처가 생기면 (c) 검토 |
| D6-2 | 매진 후 요청 차단 | (a) 그대로 Lua 실행 (O(1)) (b) 애플리케이션 로컬 "매진" 플래그로 Redis 호출 전 차단 | **✅ 확정(기본안, 2026-11-17): (a)**. 8단계 부하 테스트에서 매진 구간 Redis 부하를 측정한 뒤 재검토 |
| D6-3 | `/me` 발급 직후 미반영 | (a) DB 만 조회 (잠깐 비어 보일 수 있음) (b) Redis Set 으로 "발급됨" 표시 보완 | **✅ 확정(기본안, 2026-11-17): (a)**. 발급 응답(202)에 이미 결과가 있으므로 사용자 영향이 작다 |
| D6-4 | 유령 이벤트·판정 Lua 타임아웃 대응 (4단계 E-40/E-41) | **4단계 E-40/E-41 과 같은 방식 적용 필요** — 요청 ID(eventId) 기록(`coupon:{eventId}:requests` 류), 저장 컨슈머의 claim Lua(`persisted`), 보상 Lua 3결과(ALREADY_PERSISTED 면 202), Redis 예외 → 포트 예외 → 같은 요청 ID 로 보상 후 503. (v1.3 시점 문서의 `isIssued` 확인·SREM→INCR 보상은 "보상이 먼저" 순서만 가정했다) | **✅ 확정(2026-11-17, 4단계 사용자 결정 E-40·E-41 을 그대로 적용)**: 3.2·3.3 반영. 이름: 포트·Lua 는 `couponEventId`(쿠폰 이벤트)·`requestId`(요청 ID), 메시지 필드는 기존 `CouponIssued.eventId` 를 요청 ID 로 쓴다 (common 변경 없음) |

### 설계 수정 필요
| 대상 문서 | 내용 |
|-----------|------|
| [coupon-issue.md](../../design/05-features/coupon-issue.md) 3장 | 저장 컨슈머가 **저장 전 Redis `SISMEMBER` 로 보상된 발급을 걸러낸다**는 단계 추가. 쿠폰은 추첨의 건수 비교 같은 안전장치가 없어, 이 확인이 없으면 유령 이벤트가 그대로 **초과 발급**으로 이어진다 → 설계 반영 완료 (2026-11-16) |
| [coupon-issue.md](../../design/05-features/coupon-issue.md) API 표 | 재초기화 API `POST /api/coupons/events/{eventId}/initialize` (ADMIN) 추가 (본문에는 언급되어 있으나 API 표에 없음) → 설계 반영 완료 (2026-11-16) |
| [coupon-issue.md](../../design/05-features/coupon-issue.md) 발급 Lua 표 | 결과 `NOT_FOUND`(Redis meta 없음) 추가 → 설계 반영 완료 (2026-11-16) |
| [architecture.md](../../design/02-architecture/architecture.md) 6장 | 인증 제외 경로에 `GET /api/coupons/events/*` 추가 (쿠폰 이벤트 조회는 공개) → 설계 반영 완료 (2026-11-16) |
| [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.4 | (v1.3 시점) `CouponGate` 메서드(`initialize`, `isIssued`, `remaining`), `CouponSaveService` 의 `CouponGate` 의존 추가 → 설계 반영 완료 (2026-11-16). v1.4 에서 `isIssued` → `claimForSave` |
| coupon-issue.md v1.2 · architecture.md v1.8 · concurrency-consistency.md v1.5 · class-diagram.md v2.6 · sequence-diagram.md v1.4 | D6-4 (요청 ID·claim·보상 3결과·Redis 예외) → 설계 반영 완료 (2026-11-17, 착수 전) |
| class-diagram.md v2.7 | 구현 결과(`AdminGuard`, 리스너 id, 발행 어댑터 예외 범위, DLT 설정) → 설계 반영 완료 (2026-11-17) |

### 리스크
- **대기열이 없어 피크 요청이 전부 Redis 로**: 응모와 달리 유입 조절이 없다. 같은 Redis 를 쓰는 응모·대기열·랭킹이 쿠폰 오픈 순간 영향을 받을 수 있다 → 8단계에서 쿠폰 스파이크와 드로우 응모를 동시에 걸어 측정.
- **Redis 초기화 실패 후 발급 시작**: 등록 시 초기화가 실패하면 발급 요청이 `EVENT_NOT_FOUND` 로 거부된다. 오버 발급 방향이 아니라 서비스 불가 방향이므로 정합성은 안전. 재초기화 API 로 복구.
- **Redis 유실**: AOF 없이 재시작되면 잔여 수량·발급 Set 이 사라져 재초기화 시 초과 발급 가능. 0단계 AOF 설정이 전제.
- **DLT 적재분 방치**: DLT 로 간 발급은 Redis 에만 있어 사용자 `/me` 에 영원히 안 보인다. DLT 를 ERROR 로그로 감지하고 재처리하는 절차가 필요하다 (8단계 관측 구성 시 알림 대상).
- **유령 이벤트·판정 Lua 타임아웃 (E-40/E-41)**: v1.4 에서 claim·요청 ID·보상 3결과로 해소 (D6-4). 남는 경우는 **보상 자체가 실패**한 경우뿐이며, 수량 한 자리가 새는 언더 발급 방향이라 초과 발급은 아니다 (ERROR 로그). 판정 타임아웃 후 보상까지 실패하면 Redis 에만 발급이 남아 `/me` 에 안 보인다 — DLT 적재분과 같은 운영 대응.
- **발급 기간 종료 없음**: 요구사항에 종료 시각이 없어 meta 에 `issueStartAt` 만 있고 쿠폰 키에 TTL 이 없다. `requests`·`persisted` 는 수량 이하로 제한되므로 메모리는 이벤트당 O(수량).
- **보상까지 실패한 사용자**: 판정 타임아웃 후 보상도 실패하면 Redis `issued` 에 남아 그 사용자의 재요청은 계속 `DUPLICATE`(409)이고 `/me` 에는 안 보인다 (ERROR 로그로 운영 확인, 수동 복구 필요).
- **Kafka 응답 지연**: 발급 응답 시간에 Kafka ack 왕복이 포함된다. 동시성 테스트(T6-13)에서 소요 시간을 기록해 8단계 비교 기준으로 쓴다.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 테스트 ID |
|----------------|-----------|
| 동시성: 수량 100, 1,000명 동시 요청 → 발급 정확히 100 | T6-13 |
| 동시성: 같은 사용자 10번 동시 요청 → 1매 | T6-14 |
| 발급 시작 전 요청 → `NOT_STARTED` | T6-04, T6-08 |
| 발행 실패 → 보상 후 원상복구 | T6-05, T6-09, T6-15 |
| (추가) 보상된 발급이 DB 에 남지 않음 (초과 발급 없음) | T6-06, T6-16, T6-22 |
| (추가) 저장 실패 시 DLT | T6-23 |
| (추가) 컨슈머 재전달 멱등 | T6-11, T6-17 |
| (추가) 재초기화가 진행 중 수량을 덮어쓰지 않음 | T6-10 |
| (추가) API 4개 + 재초기화 API | T6-18 ~ T6-20 |
| 공통: `./gradlew build` 통과 (ArchUnit 포함) | T6-21 |
| 공통: 설계 수정 필요 항목 반영 + 변경 이력 | 커밋 #7 |
| 공통: D6-1 ~ D6-4 결정 및 문서 반영 | 커밋 #0 (착수 전), #7 |
| (추가) 유령 이벤트 소비가 보상보다 먼저 와도 초과 발급 없음 (E-40) | T6-24, T6-25, T6-27 |
| (추가) 판정 Lua 타임아웃 시 보상 후 503 (E-41) | T6-26, T6-28 |
| (추가) 역직렬화 실패 원본 바이트 DLT | T6-29 |
