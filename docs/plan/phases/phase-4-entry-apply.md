# 4단계 — 응모 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 발행 정책·DLT·저장 전 Redis 확인 확정 반영 (0단계 토픽 초기화에 DLT 추가됨), JSON 직렬화 클래스명을 0단계 공통 설정과 통일, 설계 반영 완료 표시 |

> 표기
> - **(구현 결정)**: 설계 문서에 없던 내용을 이 문서에서 정한 것
> - **(설계 수정 필요)**: 설계 문서와 다르게 가야 하는 것. 6장에 모아두었고, 구현 전에 설계 문서부터 고친다.

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 입장 토큰을 가진 사용자가 선착순으로 응모하고, 정원 초과·중복 없이 DB 에 저장된다 |
| 요구사항 | REQ-ENTRY-01, 02, 03, 04, 06 / NFR-01, 02, 03 |
| 브랜치 | `feat/phase-4-entry-apply` (`dev` 에서 생성) |
| 대상 모듈 | `entry-service` |

### 선행 조건 (0~3단계 산출물, 이 문서에서는 참조만)
| 산출물 | 단계 | 이 단계에서 쓰는 곳 |
|--------|------|---------------------|
| `common` 이벤트 record `DropCreated`, `EntryCreated` / 토픽 상수 `Topics` | 0 | Listener, Publisher |
| `common` testFixtures: ArchUnit `LayerRules`, Testcontainers 지원 클래스 | 0 | `ArchitectureTest`, 통합 테스트 |
| Flyway 의존성 및 설정 | 0 | `V1__create_entry_tables.sql` |
| 서비스별 에러 응답 형식 `ErrorResponse`, `GlobalExceptionHandler` 기본 틀, application 기반 예외 `BusinessException(ErrorCode)` | 0 | 에러 코드 → HTTP 매핑 |
| 토픽 `drop.created`, `entry.created` 생성 (파티션 수 포함) | 0 | Kafka |
| Gateway 의 `X-User-Id` 헤더 주입 | 1 | Controller |
| `drop.created` 발행 | 2 | 드로우 정보 수신 |
| queue 의 입장 토큰 키 `queue:{dropId}:token:{userId}` | 3 | 응모 Lua 의 토큰 검사 |

### 참고 설계 문서
- [entry-apply.md](../../design/05-features/entry-apply.md): Lua 판정 순서, 발행·보상, 저장 컨슈머, API
- [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md): D1(판정), D2(발행 실패 보상), D3(토큰 검증), D4(드로우 전파)
- [class-diagram.md](../../design/07-class-diagram/class-diagram.md): 1.1 응모, 1.3 드로우 정보 수신
- [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md): 2.2 ③ 응모, 5장 장애 시나리오
- [architecture.md](../../design/02-architecture/architecture.md): 4장 토픽, 5장 Redis 키
- [erd.md](../../design/06-erd/erd.md): entry_db
- [layered-architecture.md](../../design/02-architecture/layered-architecture.md): 4계층 규칙, ArchUnit R1~R7

---

## 2. 산출물 파일 목록

```
entry-service/
├── build.gradle                                        # 테스트 의존성 추가
└── src
    ├── main
    │   ├── java/com/queuedrop/entry
    │   │   ├── presentation
    │   │   │   ├── api
    │   │   │   │   ├── EntryController.java
    │   │   │   │   ├── UserIdHeader.java               # X-User-Id 상수 (구현 결정)
    │   │   │   │   └── response
    │   │   │   │       ├── EntryApplyResponse.java
    │   │   │   │       └── MyEntryResponse.java
    │   │   │   ├── listener
    │   │   │   │   ├── DropCreatedListener.java
    │   │   │   │   └── EntryCreatedListener.java
    │   │   │   └── advice
    │   │   │       └── EntryExceptionHandler.java      # 0단계 GlobalExceptionHandler 확장
    │   │   ├── application
    │   │   │   ├── service
    │   │   │   │   ├── DropSnapshotRegisterService.java
    │   │   │   │   ├── EntryApplyService.java
    │   │   │   │   ├── EntrySaveService.java
    │   │   │   │   └── EntryQueryService.java
    │   │   │   ├── dto
    │   │   │   │   ├── DropSnapshotRegisterCommand.java
    │   │   │   │   ├── EntryApplyCommand.java
    │   │   │   │   ├── EntryApplyResult.java
    │   │   │   │   ├── EntrySaveCommand.java
    │   │   │   │   └── MyEntryResult.java
    │   │   │   ├── port
    │   │   │   │   ├── EntryGate.java
    │   │   │   │   ├── GateResult.java
    │   │   │   │   ├── EntryEventPublisher.java
    │   │   │   │   ├── EntryCreatedPayload.java
    │   │   │   │   └── EventPublishException.java
    │   │   │   └── exception
    │   │   │       ├── EntryErrorCode.java
    │   │   │       └── EntryApplyException.java
    │   │   ├── domain
    │   │   │   ├── model
    │   │   │   │   ├── Entry.java
    │   │   │   │   ├── EntryStatus.java
    │   │   │   │   ├── ApplyOutcome.java
    │   │   │   │   ├── DropSnapshot.java
    │   │   │   │   └── SnapshotStatus.java
    │   │   │   ├── repository
    │   │   │   │   ├── EntryRepository.java
    │   │   │   │   └── DropSnapshotRepository.java
    │   │   │   └── exception
    │   │   │       └── DomainException.java            # 0단계에 없으면 여기서 생성
    │   │   └── infrastructure
    │   │       ├── persistence
    │   │       │   ├── EntryJpaEntity.java
    │   │       │   ├── EntryJpaRepository.java
    │   │       │   ├── EntryMapper.java
    │   │       │   ├── EntryRepositoryImpl.java
    │   │       │   ├── DropSnapshotJpaEntity.java
    │   │       │   ├── DropSnapshotJpaRepository.java
    │   │       │   ├── DropSnapshotMapper.java
    │   │       │   └── DropSnapshotRepositoryImpl.java
    │   │       ├── redis
    │   │       │   ├── EntryRedisKeys.java
    │   │       │   ├── EntryScripts.java               # Lua 스크립트 빈 등록
    │   │       │   └── RedisEntryGate.java
    │   │       ├── messaging
    │   │       │   └── KafkaEntryEventPublisher.java
    │   │       └── config
    │   │           ├── ClockConfig.java
    │   │           └── KafkaConsumerConfig.java        # 에러 핸들러, DLT
    │   └── resources
    │       ├── application.yml
    │       ├── scripts
    │       │   ├── entry-initialize.lua
    │       │   ├── entry-apply.lua
    │       │   └── entry-compensate.lua
    │       └── db/migration
    │           └── V1__create_entry_tables.sql
    └── test/java/com/queuedrop/entry
        ├── ArchitectureTest.java
        ├── domain/model
        │   ├── EntryTest.java
        │   └── DropSnapshotTest.java
        ├── application
        │   ├── fake
        │   │   ├── FakeEntryGate.java
        │   │   ├── FakeEntryEventPublisher.java
        │   │   ├── InMemoryEntryRepository.java
        │   │   └── InMemoryDropSnapshotRepository.java
        │   ├── EntryApplyServiceTest.java
        │   ├── EntrySaveServiceTest.java
        │   ├── DropSnapshotRegisterServiceTest.java
        │   └── EntryQueryServiceTest.java
        ├── infrastructure
        │   ├── redis/RedisEntryGateTest.java
        │   ├── persistence/EntryRepositoryImplTest.java
        │   └── messaging/KafkaEntryEventPublisherTest.java
        ├── presentation
        │   ├── api/EntryControllerTest.java
        │   └── listener/EntryCreatedListenerTest.java
        └── scenario
            ├── EntryConcurrencyTest.java
            ├── EntryCompensationScenarioTest.java
            ├── GhostEventScenarioTest.java
            └── DropCreatedRedeliveryTest.java
```

---

## 3. 구현 상세

전체 흐름은 네 갈래다. 각 흐름 안에서 domain → application → infrastructure → presentation 순서로 적는다.

| 흐름 | 시작점 | 결과 |
|------|--------|------|
| 3.1 드로우 정보 수신 | `drop.created` | `drop_snapshots` 저장 + Redis 응모 메타·정원 초기화 |
| 3.2 응모 | `POST /api/entries/drops/{dropId}` | Redis 판정 + `entry.created` 발행 (실패 시 보상) |
| 3.3 저장 | `entry.created` | `entries` 저장 (멱등) |
| 3.4 조회 | `GET /api/entries/me`, `GET /api/entries/drops/{dropId}/me` | 내 응모 내역 |

### 3.0 공통

#### Redis 키 (`infrastructure/redis/EntryRedisKeys`)
[architecture.md](../../design/02-architecture/architecture.md) 5장과 정확히 같은 이름을 쓴다.

| 키 | 타입 | 이 서비스의 권한 |
|----|------|------------------|
| `entry:{dropId}:meta` | Hash (`state`, `capacity`, `openAt`, `closeAt`) | 쓰기 |
| `entry:{dropId}:remaining` | String | 쓰기 |
| `entry:{dropId}:applicants` | Set | 쓰기 |
| `entry:{dropId}:seq` | String | 쓰기 |
| `queue:{dropId}:token:{userId}` | String(TTL) | **읽기 전용** (queue 소유) |

```java
package com.queuedrop.entry.infrastructure.redis;

final class EntryRedisKeys {

    private EntryRedisKeys() {}

    static String meta(long dropId)       { return "entry:" + dropId + ":meta"; }
    static String remaining(long dropId)  { return "entry:" + dropId + ":remaining"; }
    static String applicants(long dropId) { return "entry:" + dropId + ":applicants"; }
    static String seq(long dropId)        { return "entry:" + dropId + ":seq"; }

    /** queue-service 소유 키. 읽기만 한다. 형식 변경 시 queue-service 와 함께 수정 (architecture.md 5장). */
    static String queueToken(long dropId, long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }
}
```
- 키 문자열의 `{dropId}` 는 설계 문서의 자리표시자다. 실제 키에는 중괄호 없이 `entry:42:meta` 처럼 들어간다.
- queue 토큰 키 형식은 이 클래스에 중복으로 정의한다 **(구현 결정)**. `common` 으로 옮기면 서비스 간 결합이 코드로 드러나는 장점이 있지만, 키 하나 때문에 common 에 Redis 개념이 들어가는 게 더 크다고 판단했다.

#### 시각 주입 (`infrastructure/config/ClockConfig`)
```java
@Configuration
class ClockConfig {
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
```
- application 서비스는 `Clock` 을 주입받아 `now` 를 만든다. 테스트에서는 `Clock.fixed(...)` 로 바꾼다.
- domain 은 `Clock` 도 모른다. 시각은 항상 인자로 받는다.

#### application.yml (전문)
```yaml
server:
  port: 8085

spring:
  application:
    name: entry-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/entry_db
    username: ${DB_USERNAME:queuedrop}
    password: ${DB_PASSWORD:queuedrop}
  jpa:
    hibernate:
      ddl-auto: validate          # 스키마는 Flyway 가 관리 (0단계 결정)
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      timeout: 1s
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all
      retries: 2147483647
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer   # 0단계 공통 설정과 동일, 구현 시 확인 (아래 참고)
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        linger.ms: 0
        request.timeout.ms: 1000
        delivery.timeout.ms: 3000
    consumer:
      group-id: entry-service
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer  # 0단계 공통 설정과 동일, 구현 시 확인
        spring.json.trusted.packages: com.queuedrop.common.event
    listener:
      ack-mode: record
      concurrency: 3              # 0단계 토픽 파티션 수와 맞춘다

entry:
  publish:
    await-timeout: 3500ms         # delivery.timeout.ms(3000) + 여유
```

**Producer 설정 근거**
| 설정 | 값 | 이유 |
|------|----|------|
| `acks` | `all` | 리더와 ISR 모두 기록해야 성공. "성공 응답 = 이벤트 기록" 불변식 ([D2](../../design/04-non-functional/concurrency-consistency.md)) |
| `enable.idempotence` | `true` | producer 내부 재시도로 생기는 중복 기록을 브로커가 제거 |
| `retries` | `MAX_INT` | 재시도 횟수가 아니라 `delivery.timeout.ms` 로 전체 시간을 제한 |
| `request.timeout.ms` / `delivery.timeout.ms` | 1000 / 3000 | send 결과가 **3초 안에 반드시 확정**되게 한다. `get(timeout)` 만 걸면 future 는 타임아웃돼도 producer 는 최대 120초(기본값) 동안 계속 재시도해서, 보상 후에 뒤늦게 기록되는 유령 이벤트가 생기기 쉽다 |
| `linger.ms` | 0 | 응답 지연에 배치 대기 시간을 넣지 않는다. 8단계 부하 테스트에서 재조정 |

> **설계와 다른 점 (설계 수정 필요 ①)**: 설계는 "`get(3s)` + 애플리케이션 재시도 2회" 다. 이 문서는 **애플리케이션 재시도를 없애고 producer 내부 재시도(멱등) + `delivery.timeout.ms=3000`** 으로 바꾼다. 애플리케이션이 같은 레코드를 다시 send 하면 멱등 producer 도 중복을 막지 못하고(별개 레코드로 취급), 3초 × 3회면 응답이 최대 9초까지 늘어난다.

**Consumer 설정 근거**
| 설정 | 값 | 이유 |
|------|----|------|
| `enable-auto-commit` | `false` | 자동 커밋은 처리 전에 오프셋이 커밋될 수 있다 → 저장 전 장애 시 유실 |
| `ack-mode` | `record` | 레코드 하나를 **DB 저장 성공 후** 커밋. at-least-once + 컨슈머 멱등으로 정확히 한 번처럼 동작 |
| `ErrorHandlingDeserializer` | - | 역직렬화 실패 레코드 하나가 파티션 전체를 막지 않게 한다 |
| 타입 정보 | `__TypeId__` 헤더 (JSON serializer 기본) | 모든 서비스가 같은 `common` 클래스를 쓰므로 헤더의 FQCN 이 그대로 맞는다. `trusted.packages` 로 범위를 제한 |

> Spring Kafka 4.x(Boot 4.1.2 관리 버전)는 Jackson 3 기반 `JacksonJsonSerializer` / `JacksonJsonDeserializer` 를 새로 두고 기존 `JsonSerializer` 계열을 deprecated 했을 가능성이 있다. **구현 시 확인**: deprecated 면 새 클래스로 바꾸고 프로퍼티 키도 함께 확인한다.

#### Kafka 에러 핸들러 (`infrastructure/config/KafkaConsumerConfig`) (구현 결정)
```java
@Configuration
class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template);   // {topic}.DLT 로 보냄
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }
}
```
- 저장 실패(DB 장애 등)는 1초 간격 3번 재시도 후 `entry.created.DLT` 로 보낸다.
- DLT 토픽 `drop.created.DLT`, `entry.created.DLT` 는 0단계 토픽 초기화(`create-topics.sh`)에서 만든다 (확정, 설계 수정 ② 반영). 토픽 이름은 `Topics.*_DLT` 상수를 쓴다.
- DLT 로 간 응모는 Redis 에는 있고 DB 에는 없는 상태라, 5단계 추첨의 건수 비교에서 불일치로 드러난다. 그때 DLT 를 다시 처리하면 된다.
- Boot 4 에서 `DefaultErrorHandler` 빈 하나만 두면 기본 컨테이너 팩토리에 자동 적용되는지 **구현 시 확인**. 안 되면 `ConcurrentKafkaListenerContainerFactory` 에 직접 설정한다.

#### Flyway 마이그레이션 (`db/migration/V1__create_entry_tables.sql`, 전문)
```sql
-- drop.created 로 받은 드로우 정보의 읽기 전용 사본 (erd.md entry_db)
CREATE TABLE drop_snapshots (
    drop_id        BIGINT       PRIMARY KEY,
    entry_capacity INT          NOT NULL,
    winner_count   INT          NOT NULL,
    open_at        TIMESTAMPTZ  NOT NULL,
    close_at       TIMESTAMPTZ  NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    CONSTRAINT ck_drop_snapshots_capacity CHECK (entry_capacity >= winner_count AND winner_count >= 1),
    CONSTRAINT ck_drop_snapshots_period   CHECK (open_at < close_at),
    CONSTRAINT ck_drop_snapshots_status   CHECK (status IN ('OPEN', 'CLOSING', 'DRAWN'))
);

CREATE TABLE entries (
    id          BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drop_id     BIGINT       NOT NULL,
    user_id     BIGINT       NOT NULL,
    entry_seq   INT          NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_entries_drop_user UNIQUE (drop_id, user_id),     -- 1인 1드로우 1응모
    CONSTRAINT uk_entries_drop_seq  UNIQUE (drop_id, entry_seq),   -- 응모 번호 중복 없음
    CONSTRAINT ck_entries_status    CHECK (status IN ('APPLIED', 'WON', 'LOST'))
);

-- 내 응모 목록 조회 (GET /api/entries/me)
CREATE INDEX idx_entries_user_applied ON entries (user_id, applied_at DESC);
```
- `entries.drop_id → drop_snapshots.drop_id` FK 는 두지 않는다. ERD 의 "논리 참조" 를 따른다.
- `drop_id`, `entry_seq` 에는 따로 인덱스를 만들지 않는다. 두 UK 가 이미 `(drop_id, ...)` 선두 인덱스 역할을 해서, 5단계의 `COUNT` 와 `ORDER BY entry_seq` 조회에 그대로 쓰인다.
- CHECK 제약은 ERD 에 없다 **(구현 결정)**. 도메인 검증이 1차, DB 제약이 2차 방어선이다.

---

### 3.1 드로우 정보 수신

```
drop.created → DropCreatedListener → DropSnapshotRegisterService
                                       ├─ DropSnapshotRepository.saveIfAbsent   (DB)
                                       └─ EntryGate.initialize                   (Redis, NX)
```

#### domain

**`SnapshotStatus`**: 추첨 진행 상태. 이 단계에서는 `OPEN` 만 쓰고, `CLOSING` / `DRAWN` 은 5단계에서 쓴다.
```java
package com.queuedrop.entry.domain.model;

public enum SnapshotStatus { OPEN, CLOSING, DRAWN }
```

**`DropSnapshot`**: product 드로우의 사본. 생성 시 불변식을 검증한다.
```java
package com.queuedrop.entry.domain.model;

public final class DropSnapshot {

    private final long dropId;
    private final int entryCapacity;
    private final int winnerCount;
    private final Instant openAt;
    private final Instant closeAt;
    private final SnapshotStatus status;

    private DropSnapshot(long dropId, int entryCapacity, int winnerCount,
                         Instant openAt, Instant closeAt, SnapshotStatus status) {
        // 필드 대입
    }

    /** drop.created 수신 시 생성. 항상 OPEN 으로 시작한다. */
    public static DropSnapshot register(long dropId, int entryCapacity, int winnerCount,
                                        Instant openAt, Instant closeAt) {
        if (winnerCount < 1 || entryCapacity < winnerCount) {
            throw new DomainException("entryCapacity >= winnerCount >= 1 이어야 합니다");
        }
        if (!openAt.isBefore(closeAt)) {
            throw new DomainException("openAt < closeAt 이어야 합니다");
        }
        return new DropSnapshot(dropId, entryCapacity, winnerCount, openAt, closeAt, SnapshotStatus.OPEN);
    }

    /** 저장소에서 복원할 때만 사용 (Mapper 전용). 검증하지 않는다. */
    public static DropSnapshot restore(long dropId, int entryCapacity, int winnerCount,
                                       Instant openAt, Instant closeAt, SnapshotStatus status) {
        return new DropSnapshot(dropId, entryCapacity, winnerCount, openAt, closeAt, status);
    }

    // getter (Lombok @Getter 허용: compileOnly)
}
```

**`DropSnapshotRepository`**: 영속성 포트.
```java
package com.queuedrop.entry.domain.repository;

public interface DropSnapshotRepository {

    /** 이미 있으면 저장하지 않고 false. drop.created 재수신 대비 멱등 저장. */
    boolean saveIfAbsent(DropSnapshot snapshot);

    Optional<DropSnapshot> findById(long dropId);
}
```
- 클래스 다이어그램 1.3 에는 메서드가 정의되어 있지 않다. `saveIfAbsent` 이름은 **(구현 결정)**.

#### application

**`DropSnapshotRegisterCommand`**
```java
package com.queuedrop.entry.application.dto;

public record DropSnapshotRegisterCommand(
        long dropId, int entryCapacity, int winnerCount, Instant openAt, Instant closeAt) {}
```

**`DropSnapshotRegisterService`**: DB 저장 후 Redis 초기화. 둘 다 멱등이라 어느 단계에서 실패해도 재전달로 복구된다.
```java
package com.queuedrop.entry.application.service;

@Service
@RequiredArgsConstructor
public class DropSnapshotRegisterService {

    private final DropSnapshotRepository dropSnapshotRepository;
    private final EntryGate entryGate;

    public void register(DropSnapshotRegisterCommand command) {
        DropSnapshot snapshot = DropSnapshot.register(
                command.dropId(), command.entryCapacity(), command.winnerCount(),
                command.openAt(), command.closeAt());

        dropSnapshotRepository.saveIfAbsent(snapshot);       // ① DB (자체 트랜잭션)

        entryGate.initialize(command.dropId(), command.entryCapacity(),   // ② Redis (NX)
                command.openAt(), command.closeAt());
    }
}
```
- **순서와 실패 처리** (구현 결정)
  | 실패 지점 | 상태 | 재전달 시 |
  |-----------|------|-----------|
  | ① DB 저장 | 아무것도 없음 | ①부터 다시 |
  | ② Redis 초기화 | DB 만 있음 | ① 은 이미 있어서 건너뜀, ② 실행 |
- 메서드에 `@Transactional` 을 걸지 않는다. Redis 호출을 DB 트랜잭션 안에 두면, 커밋 실패 시 Redis 만 초기화된 상태가 남는다.
- 도메인 검증 실패(`DomainException`)는 재시도해도 같은 결과라서 Listener 가 잡아 로그만 남긴다 (3.1 presentation 참고).

**`EntryGate.initialize`**: 포트 정의는 3.2 에 전체를 적는다.

#### infrastructure

**`entry-initialize.lua` (전문)**
```lua
-- 드로우 응모 상태 초기화. 이미 초기화된 드로우는 건드리지 않는다 (drop.created 재수신 대비).
-- KEYS[1] = entry:{dropId}:meta
-- KEYS[2] = entry:{dropId}:remaining
-- ARGV[1] = capacity
-- ARGV[2] = openAt (epoch ms)
-- ARGV[3] = closeAt (epoch ms)
-- return  : 1 = 초기화함, 0 = 이미 있음

if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

redis.call('HSET', KEYS[1],
    'state', 'OPEN',
    'capacity', ARGV[1],
    'openAt', ARGV[2],
    'closeAt', ARGV[3])
redis.call('SET', KEYS[2], ARGV[1])
return 1
```
| 구분 | 값 |
|------|-----|
| KEYS | `meta`, `remaining` |
| ARGV | `capacity`, `openAt(ms)`, `closeAt(ms)` |
| 반환 | `1` 초기화 / `0` 이미 존재 |

- 설계의 "`SET NX` 로 초기화" 를 **meta 존재 여부를 보는 Lua 하나**로 구현한다 **(구현 결정)**. `SET remaining NX` 와 `HSETNX` 를 따로 실행하면, 둘 사이에 장애가 났을 때 한쪽만 초기화된 상태가 남는다.
- `seq` 키는 만들지 않는다. `INCR` 은 없는 키를 0 으로 보고 1을 반환한다.
- `state` 는 오픈 전이어도 `OPEN` 으로 넣는다. 오픈 전 응모는 응모 Lua 의 시각 검사(`now < openAt`)가 막는다.

**`DropSnapshotJpaEntity`, `DropSnapshotMapper`, `DropSnapshotRepositoryImpl`**
```java
package com.queuedrop.entry.infrastructure.persistence;

@Entity
@Table(name = "drop_snapshots")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class DropSnapshotJpaEntity {
    @Id
    private Long dropId;                  // product 의 id 를 그대로 쓴다 (자동 생성 아님)
    private int entryCapacity;
    private int winnerCount;
    private Instant openAt;
    private Instant closeAt;
    @Enumerated(EnumType.STRING)
    private SnapshotStatus status;        // infrastructure → domain 의존은 허용 (R1~R5)
    // 전체 필드 생성자
}

interface DropSnapshotJpaRepository extends JpaRepository<DropSnapshotJpaEntity, Long> {

    @Modifying
    @Query(value = """
            INSERT INTO drop_snapshots (drop_id, entry_capacity, winner_count, open_at, close_at, status)
            VALUES (:#{#e.dropId}, :#{#e.entryCapacity}, :#{#e.winnerCount},
                    :#{#e.openAt}, :#{#e.closeAt}, :#{#e.status.name()})
            ON CONFLICT (drop_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("e") DropSnapshotJpaEntity entity);
}

@Repository
@RequiredArgsConstructor
class DropSnapshotRepositoryImpl implements DropSnapshotRepository {

    private final DropSnapshotJpaRepository jpa;

    @Override
    @Transactional
    public boolean saveIfAbsent(DropSnapshot snapshot) {
        return jpa.insertIfAbsent(DropSnapshotMapper.toEntity(snapshot)) == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<DropSnapshot> findById(long dropId) {
        return jpa.findById(dropId).map(DropSnapshotMapper::toDomain);
    }
}
```
- `@Id` 를 직접 넣는 엔티티에 `save()` 를 쓰면 Spring Data 가 `merge` 로 처리해서 기존 행을 덮어쓴다. 그래서 **`INSERT ... ON CONFLICT DO NOTHING`** 으로 "없을 때만 저장" 을 표현한다 **(구현 결정)**.
- SpEL 파라미터(`:#{#e.dropId}`)가 Boot 4 / Spring Data 4 에서 그대로 동작하는지 **구현 시 확인**. 안 되면 필드별 `@Param` 으로 바꾼다.

**`RedisEntryGate.initialize`**: 3.2 infrastructure 에 함께 적는다.

#### presentation

**`DropCreatedListener`**: `common` 메시지를 Command 로 바꿔 application 을 호출한다.
```java
package com.queuedrop.entry.presentation.listener;

@Slf4j
@Component
@RequiredArgsConstructor
public class DropCreatedListener {

    private final DropSnapshotRegisterService registerService;

    @KafkaListener(id = "entry-drop-created", topics = Topics.DROP_CREATED)
    public void on(DropCreated message) {
        try {
            registerService.register(new DropSnapshotRegisterCommand(
                    message.dropId(), message.entryCapacity(), message.winnerCount(),
                    message.openAt(), message.closeAt()));
        } catch (InvalidDropSnapshotException e) {
            log.error("잘못된 드로우 정보, 재시도하지 않음: dropId={}", message.dropId(), e);
        }
    }
}
```
- presentation 은 `DomainException` 을 직접 잡을 수 없다(R4). 그래서 `DropSnapshotRegisterService` 가 `DomainException` 을 잡아 application 예외 **`InvalidDropSnapshotException`**(`application/exception`)으로 바꿔 던진다 **(구현 결정)**.
- DB·Redis 장애 같은 나머지 예외는 그대로 던져서 `DefaultErrorHandler` 가 재시도하게 둔다.

---

### 3.2 응모

```
POST /api/entries/drops/{dropId}
  → EntryController → EntryApplyService
      ├─ EntryGate.tryApply        (Redis Lua: 마감·토큰·중복·잔여 검사 + DECR + SADD + INCR seq)
      ├─ EntryEventPublisher       (Kafka entry.created, ack 대기)
      └─ 실패 시 EntryGate.compensate (Redis Lua: SREM + INCR remaining)
```

#### domain

**`ApplyOutcome`**: 응모 판정 결과. 규칙 자체는 Lua 에 있고 domain 은 결과 타입만 갖는다 ([layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.3).
```java
package com.queuedrop.entry.domain.model;

public enum ApplyOutcome { OK, CLOSED, NO_TOKEN, DUPLICATE, SOLD_OUT }
```

**`EntryStatus`**
```java
public enum EntryStatus { APPLIED, WON, LOST }
```

**`Entry`**: 응모 한 건. 이 단계에서는 생성만 하고, 상태 변경(`win`, `lose`)은 5단계에서 추가한다.
```java
package com.queuedrop.entry.domain.model;

public final class Entry {

    private final Long id;            // 저장 전 null
    private final long dropId;
    private final long userId;
    private final int entrySeq;
    private final EntryStatus status;
    private final Instant appliedAt;

    private Entry(Long id, long dropId, long userId, int entrySeq,
                  EntryStatus status, Instant appliedAt) { /* 대입 */ }

    /** Redis 판정을 통과한 응모를 기록용 모델로 만든다. */
    public static Entry apply(long dropId, long userId, int entrySeq, Instant appliedAt) {
        if (entrySeq < 1) {
            throw new DomainException("entrySeq 는 1 이상이어야 합니다");
        }
        Objects.requireNonNull(appliedAt, "appliedAt");
        return new Entry(null, dropId, userId, entrySeq, EntryStatus.APPLIED, appliedAt);
    }

    /** 저장소 복원 전용 (Mapper). */
    public static Entry restore(Long id, long dropId, long userId, int entrySeq,
                                EntryStatus status, Instant appliedAt) {
        return new Entry(id, dropId, userId, entrySeq, status, appliedAt);
    }

    // getter
}
```

**`EntryRepository`**: 영속성 포트.
```java
package com.queuedrop.entry.domain.repository;

public interface EntryRepository {

    /**
     * (dropId, userId) 응모가 없을 때만 저장한다.
     * @return 저장했으면 true, 같은 사용자 응모가 이미 있으면 false
     * @throws 응모 번호(entrySeq) 충돌 등 그 밖의 제약 위반은 예외 (정상이라면 일어나지 않음)
     */
    boolean saveIfAbsent(Entry entry);

    Optional<Entry> findByDropIdAndUserId(long dropId, long userId);

    List<Entry> findAllByUserId(long userId);
}
```
- **설계 수정 필요 ③**: 클래스 다이어그램 1.1 은 `save(Entry) Entry` + `existsByDropIdAndUserId` 다. "exists 확인 후 save" 는 확인과 저장 사이에 경합이 생기고, 유니크 위반 예외(Spring 예외)를 application 이 알게 된다. 그래서 **`saveIfAbsent(Entry) boolean` 하나로 합친다.**

#### application

**Command / Result**
```java
package com.queuedrop.entry.application.dto;

public record EntryApplyCommand(long dropId, long userId) {}

public record EntryApplyResult(int entrySeq) {}
```

**포트: `EntryGate`, `GateResult`** (`application/port`)
```java
package com.queuedrop.entry.application.port;

/** Redis 기반 선착순 판정 게이트. 판정·보상·초기화를 원자적으로 수행한다. */
public interface EntryGate {

    GateResult tryApply(long dropId, long userId, Instant now);

    /** 판정을 되돌린다. 응모자에 없으면 아무것도 하지 않는다 (두 번 호출해도 안전). seq 는 되돌리지 않는다. */
    void compensate(long dropId, long userId);

    boolean isApplicant(long dropId, long userId);

    /** 응모 메타·정원 초기화. 이미 초기화돼 있으면 아무것도 하지 않는다. */
    void initialize(long dropId, int capacity, Instant openAt, Instant closeAt);
}

/** entrySeq 는 outcome == OK 일 때만 값이 있다. */
public record GateResult(ApplyOutcome outcome, Integer entrySeq) {
    public static GateResult ok(int entrySeq)                 { return new GateResult(ApplyOutcome.OK, entrySeq); }
    public static GateResult rejected(ApplyOutcome outcome)   { return new GateResult(outcome, null); }
    public boolean isOk()                                     { return outcome == ApplyOutcome.OK; }
}
```
- `GateResult` 가 domain enum `ApplyOutcome` 을 갖는 건 허용된다. R6 은 **application.service 의 public 시그니처**만 막는다. 포트는 application 안에서만 쓰여 presentation 에 닿지 않는다.

**포트: `EntryEventPublisher`, `EntryCreatedPayload`, `EventPublishException`**
```java
package com.queuedrop.entry.application.port;

public interface EntryEventPublisher {
    /**
     * 브로커 기록이 확인될 때까지 기다린다 (동기).
     * @throws EventPublishException 정해진 시간 안에 기록이 확인되지 않음
     */
    void publishCreated(EntryCreatedPayload payload);
}

/** application 자체 DTO. common 의 EntryCreated 메시지로의 변환은 infrastructure 가 맡는다. */
public record EntryCreatedPayload(
        String eventId, long dropId, long userId, int entrySeq, Instant appliedAt) {}

public class EventPublishException extends RuntimeException {
    public EventPublishException(String message, Throwable cause) { super(message, cause); }
}
```

**에러 코드·예외** (`application/exception`)
```java
package com.queuedrop.entry.application.exception;

public enum EntryErrorCode implements ErrorCode {        // ErrorCode: 0단계 공통 인터페이스 (가정)
    CLOSED("응모 기간이 아닙니다"),
    NO_TOKEN("입장 토큰이 없거나 만료되었습니다"),
    DUPLICATE("이미 응모한 드로우입니다"),
    SOLD_OUT("응모 정원이 마감되었습니다"),
    TEMPORARILY_FAILED("일시적인 오류입니다. 다시 시도해 주세요"),
    ENTRY_NOT_FOUND("응모 내역이 없습니다");              // (구현 결정) 단건 조회용

    private final String message;

    EntryErrorCode(String message) { this.message = message; }

    /** domain 판정 결과 → application 에러 코드. 변환은 여기 한 곳에서만 한다. */
    public static EntryErrorCode from(ApplyOutcome outcome) {
        return switch (outcome) {
            case CLOSED -> CLOSED;
            case NO_TOKEN -> NO_TOKEN;
            case DUPLICATE -> DUPLICATE;
            case SOLD_OUT -> SOLD_OUT;
            case OK -> throw new IllegalArgumentException("OK 는 에러가 아닙니다");
        };
    }
}

public class EntryApplyException extends BusinessException {   // BusinessException: 0단계 (가정)
    public EntryApplyException(EntryErrorCode code)                  { super(code); }
    public EntryApplyException(EntryErrorCode code, Throwable cause) { super(code, cause); }
}
```

> **ApplyOutcome → 예외 변환 위치**: `EntryApplyService` 가 `EntryErrorCode.from(outcome)` 으로 변환해 `EntryApplyException` 을 던진다. presentation 은 `EntryErrorCode` 만 보고 HTTP 상태를 정한다. `ApplyOutcome` 은 application 밖으로 나가지 않는다.

**`EntryApplyService`**: 판정 → 발행 → (실패 시) 보상.
```java
package com.queuedrop.entry.application.service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EntryApplyService {

    private final EntryGate entryGate;
    private final EntryEventPublisher eventPublisher;
    private final Clock clock;

    public EntryApplyResult apply(EntryApplyCommand command) {
        Instant now = clock.instant();

        GateResult gate = entryGate.tryApply(command.dropId(), command.userId(), now);
        if (!gate.isOk()) {
            throw new EntryApplyException(EntryErrorCode.from(gate.outcome()));
        }

        EntryCreatedPayload payload = new EntryCreatedPayload(
                UUID.randomUUID().toString(), command.dropId(), command.userId(),
                gate.entrySeq(), now);

        try {
            eventPublisher.publishCreated(payload);
        } catch (EventPublishException e) {
            compensateQuietly(command);
            throw new EntryApplyException(EntryErrorCode.TEMPORARILY_FAILED, e);
        }

        return new EntryApplyResult(gate.entrySeq());
    }

    private void compensateQuietly(EntryApplyCommand command) {
        try {
            entryGate.compensate(command.dropId(), command.userId());
        } catch (RuntimeException e) {
            // 보상 실패: 정원 한 자리가 새는 쪽(언더 응모)이라 정합성 위반은 아니다 (D2). 알림 대상.
            log.error("보상 실패 dropId={} userId={}", command.dropId(), command.userId(), e);
        }
    }
}
```
- `@Transactional` 이 없다. 이 유스케이스는 DB 를 쓰지 않는다 (응답 경로에서 DB 제외, D1).
- `appliedAt` 은 판정에 쓴 `now` 와 같은 값이다. 7단계 랭킹의 주차 계산이 이 값을 쓴다.

#### infrastructure

**`entry-apply.lua` (전문)**
```lua
-- 선착순 응모 원자 판정 (entry-apply.md 3장 표의 순서 그대로)
-- KEYS[1] = entry:{dropId}:meta
-- KEYS[2] = entry:{dropId}:remaining
-- KEYS[3] = entry:{dropId}:applicants
-- KEYS[4] = entry:{dropId}:seq
-- KEYS[5] = queue:{dropId}:token:{userId}     (queue-service 소유, 읽기만)
-- ARGV[1] = userId
-- ARGV[2] = now (epoch ms, 애플리케이션 시각)
-- return  : { 'OK', entrySeq } 또는 { 'CLOSED' | 'NO_TOKEN' | 'DUPLICATE' | 'SOLD_OUT' }

-- 1. 마감 여부: state == OPEN 이고 openAt <= now < closeAt
local meta = redis.call('HMGET', KEYS[1], 'state', 'openAt', 'closeAt')
if not meta[1] then
    return { 'CLOSED' }                      -- 초기화 전 드로우 (구현 결정)
end
local now = tonumber(ARGV[2])
if meta[1] ~= 'OPEN' or now < tonumber(meta[2]) or now >= tonumber(meta[3]) then
    return { 'CLOSED' }
end

-- 2. 입장 토큰
if redis.call('EXISTS', KEYS[5]) == 0 then
    return { 'NO_TOKEN' }
end

-- 3. 중복 응모
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
    return { 'DUPLICATE' }
end

-- 4. 잔여 정원
local remaining = tonumber(redis.call('GET', KEYS[2]) or '0')
if remaining <= 0 then
    return { 'SOLD_OUT' }
end

-- 5. 차감 + 응모자 등록
redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])

-- 6. 응모 번호: 잔여 정원과 무관한 카운터 (증가만, 보상 시에도 되돌리지 않음)
local seq = redis.call('INCR', KEYS[4])
return { 'OK', seq }
```
| 구분 | 값 |
|------|-----|
| KEYS | `meta`, `remaining`, `applicants`, `seq`, `queueToken` |
| ARGV | `userId`, `now(ms)` |
| 반환 | `['OK', seq]` / `['CLOSED']` / `['NO_TOKEN']` / `['DUPLICATE']` / `['SOLD_OUT']` |

- 모든 명령이 O(1) 이다. 반복문을 넣지 않는다 (Lua 실행 중에는 Redis 전체가 멈춘다).
- 초기화 전 드로우(`meta` 없음)를 `CLOSED` 로 처리하는 건 **(구현 결정)** 이다. 에러 코드를 늘리지 않고 "지금은 응모할 수 없음" 으로 묶었다. `drop.created` 전파가 늦은 경우가 여기에 해당한다.
- `closeAt` 정각은 `CLOSED` 다 (`now >= closeAt`). 설계의 `openAt ≤ now < closeAt` 과 같다.

**`entry-compensate.lua` (전문)**
```lua
-- 응모 판정 되돌리기 (Kafka 발행 최종 실패 시)
-- KEYS[1] = entry:{dropId}:remaining
-- KEYS[2] = entry:{dropId}:applicants
-- ARGV[1] = userId
-- return  : 1 = 되돌림, 0 = 응모자에 없음 (이미 보상됐거나 판정된 적 없음)

if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
    redis.call('INCR', KEYS[1])
    return 1
end
return 0
```
| 구분 | 값 |
|------|-----|
| KEYS | `remaining`, `applicants` |
| ARGV | `userId` |
| 반환 | `1` 되돌림 / `0` 대상 없음 |

- `SREM` 결과로 `INCR` 여부를 정하므로 **두 번 호출해도 정원이 두 번 늘지 않는다.**
- `seq` 는 건드리지 않는다 (번호 재사용 방지, [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 5.2).

**`EntryScripts`**: 스크립트를 빈으로 한 번만 로드한다. `RedisTemplate.execute(script, ...)` 는 EVALSHA 를 먼저 시도하고, 스크립트 캐시가 없으면 EVAL 로 다시 보낸다.
```java
package com.queuedrop.entry.infrastructure.redis;

@Configuration
class EntryScripts {

    @Bean
    RedisScript<Long> entryInitializeScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-initialize.lua"), Long.class);
    }

    @Bean
    @SuppressWarnings("rawtypes")
    RedisScript<List> entryApplyScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-apply.lua"), List.class);
    }

    @Bean
    RedisScript<Long> entryCompensateScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-compensate.lua"), Long.class);
    }
}
```

**`RedisEntryGate`**: `EntryGate` 구현.
```java
package com.queuedrop.entry.infrastructure.redis;

@Component
@RequiredArgsConstructor
class RedisEntryGate implements EntryGate {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> entryInitializeScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> entryApplyScript;
    private final RedisScript<Long> entryCompensateScript;

    @Override
    public GateResult tryApply(long dropId, long userId, Instant now) {
        List<?> result = redis.execute(entryApplyScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId),
                        EntryRedisKeys.applicants(dropId), EntryRedisKeys.seq(dropId),
                        EntryRedisKeys.queueToken(dropId, userId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()));

        ApplyOutcome outcome = ApplyOutcome.valueOf((String) result.get(0));
        if (outcome != ApplyOutcome.OK) {
            return GateResult.rejected(outcome);
        }
        return GateResult.ok(Math.toIntExact((Long) result.get(1)));
    }

    @Override
    public void compensate(long dropId, long userId) {
        redis.execute(entryCompensateScript,
                List.of(EntryRedisKeys.remaining(dropId), EntryRedisKeys.applicants(dropId)),
                String.valueOf(userId));
    }

    @Override
    public boolean isApplicant(long dropId, long userId) {
        return Boolean.TRUE.equals(
                redis.opsForSet().isMember(EntryRedisKeys.applicants(dropId), String.valueOf(userId)));
    }

    @Override
    public void initialize(long dropId, int capacity, Instant openAt, Instant closeAt) {
        redis.execute(entryInitializeScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId)),
                String.valueOf(capacity),
                String.valueOf(openAt.toEpochMilli()), String.valueOf(closeAt.toEpochMilli()));
    }
}
```
- Lua 가 반환한 배열에서 문자열은 Java `String`(StringRedisTemplate 직렬화), 정수는 `Long` 으로 들어온다. 실제 타입이 `byte[]` 로 오는지 **구현 시 확인**. 그렇다면 `RedisScript` 의 결과 직렬화기를 지정하는 `execute(script, argsSerializer, resultSerializer, keys, args)` 오버로드를 쓴다.

**`KafkaEntryEventPublisher`**: `EntryEventPublisher` 구현. application DTO 를 `common` 메시지로 바꿔 보낸다.
```java
package com.queuedrop.entry.infrastructure.messaging;

@Component
class KafkaEntryEventPublisher implements EntryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Duration awaitTimeout;

    KafkaEntryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                             @Value("${entry.publish.await-timeout}") Duration awaitTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.awaitTimeout = awaitTimeout;
    }

    @Override
    public void publishCreated(EntryCreatedPayload p) {
        EntryCreated message = new EntryCreated(
                p.eventId(), p.dropId(), p.userId(), p.entrySeq(), p.appliedAt());
        String key = p.dropId() + ":" + p.userId();         // architecture.md 4장 파티션 키

        try {
            kafkaTemplate.send(Topics.ENTRY_CREATED, key, message)
                    .get(awaitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("발행 대기 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("entry.created 발행 실패", e);
        }
    }
}
```
- `get()` 의 대기 시간(3.5초)은 producer 의 `delivery.timeout.ms`(3초)보다 조금 길다. 그래서 대부분 producer 가 먼저 성공/실패를 확정하고, `TimeoutException` 은 producer 가 응답하지 않는 예외 상황에서만 난다.
- `TimeoutException` 이어도 레코드가 나중에 기록될 수 있다. 그 경우가 5.2 의 유령 이벤트고, 저장 컨슈머의 `isApplicant` 확인과 `seq` 카운터가 처리한다.

#### presentation

**`EntryController`**
```java
package com.queuedrop.entry.presentation.api;

@RestController
@RequestMapping("/api/entries")
@RequiredArgsConstructor
public class EntryController {

    private final EntryApplyService entryApplyService;
    private final EntryQueryService entryQueryService;

    @PostMapping("/drops/{dropId}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EntryApplyResponse apply(@PathVariable long dropId,
                                    @RequestHeader(UserIdHeader.NAME) long userId) {
        EntryApplyResult result = entryApplyService.apply(new EntryApplyCommand(dropId, userId));
        return EntryApplyResponse.from(result);
    }

    // 조회 API 는 3.4
}

public record EntryApplyResponse(int entrySeq) {
    public static EntryApplyResponse from(EntryApplyResult result) {
        return new EntryApplyResponse(result.entrySeq());
    }
}

public final class UserIdHeader {
    public static final String NAME = "X-User-Id";
    private UserIdHeader() {}
}
```

**`EntryExceptionHandler`**: application 에러 코드 → HTTP. 0단계의 `GlobalExceptionHandler` 형식을 따른다.
```java
package com.queuedrop.entry.presentation.advice;

@RestControllerAdvice
class EntryExceptionHandler {

    @ExceptionHandler(EntryApplyException.class)
    ResponseEntity<ErrorResponse> handle(EntryApplyException e) {
        EntryErrorCode code = (EntryErrorCode) e.getErrorCode();
        HttpStatus status = switch (code) {
            case CLOSED, NO_TOKEN, DUPLICATE, SOLD_OUT -> HttpStatus.CONFLICT;      // 409
            case TEMPORARILY_FAILED -> HttpStatus.SERVICE_UNAVAILABLE;               // 503
            case ENTRY_NOT_FOUND -> HttpStatus.NOT_FOUND;                            // 404
        };
        return ResponseEntity.status(status).body(ErrorResponse.of(code));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ErrorResponse> handleMissingUser(MissingRequestHeaderException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.of("MISSING_USER_HEADER"));
    }
}
```
| 에러 코드 | HTTP | 응답 `code` |
|-----------|------|-------------|
| `CLOSED` | 409 | `CLOSED` |
| `NO_TOKEN` | 409 | `NO_TOKEN` |
| `DUPLICATE` | 409 | `DUPLICATE` |
| `SOLD_OUT` | 409 | `SOLD_OUT` |
| `TEMPORARILY_FAILED` | 503 | `ENTRY_TEMPORARILY_FAILED` (entry-apply.md 3장 표기와 맞춤) |
| `ENTRY_NOT_FOUND` | 404 | `ENTRY_NOT_FOUND` **(구현 결정)** |
| `X-User-Id` 누락 | 400 | `MISSING_USER_HEADER` **(구현 결정)**. Gateway 를 거치면 생길 수 없지만, 내부 직접 호출 대비 |
- `NO_TOKEN` 은 403 으로 볼 수도 있지만, 클라이언트 처리가 "대기열로 돌아가기" 라서 다른 409 사유와 함께 묶었다 **(구현 결정)**.

---

### 3.3 저장

```
entry.created → EntryCreatedListener → EntrySaveService
                                         ├─ EntryGate.isApplicant  (보상된 유령 이벤트 거르기)
                                         └─ EntryRepository.saveIfAbsent
```

#### application

**`EntrySaveCommand`**
```java
public record EntrySaveCommand(long dropId, long userId, int entrySeq, Instant appliedAt) {}
```

**`EntrySaveService`**
```java
package com.queuedrop.entry.application.service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EntrySaveService {

    private final EntryRepository entryRepository;
    private final EntryGate entryGate;

    public void save(EntrySaveCommand command) {
        // ① 보상된 응모(유령 이벤트) 거르기: Redis 응모자에 없으면 저장하지 않는다
        if (!entryGate.isApplicant(command.dropId(), command.userId())) {
            log.warn("보상된 응모 이벤트 무시 dropId={} userId={} seq={}",
                    command.dropId(), command.userId(), command.entrySeq());
            return;
        }

        // ② 같은 사용자 응모가 이미 있으면 무시 (재전달, 또는 유령 + 재응모)
        Entry entry = Entry.apply(command.dropId(), command.userId(),
                command.entrySeq(), command.appliedAt());
        boolean saved = entryRepository.saveIfAbsent(entry);
        if (!saved) {
            log.debug("이미 저장된 응모 dropId={} userId={}", command.dropId(), command.userId());
        }
    }
}
```
- **저장 전 Redis 응모자 확인은 이 단계에서 확정한다.** 기본안이었던 항목을 그대로 채택한다 (6장 결정 ①).
- `@Transactional` 을 걸지 않는다. `saveIfAbsent` 가 자체 트랜잭션을 갖는다. 바깥에 트랜잭션을 두면, 그 안에서 제약 위반 예외가 나는 순간 트랜잭션이 rollback-only 로 표시되어 잡아도 커밋 시 `UnexpectedRollbackException` 이 난다.

#### infrastructure

**`EntryJpaEntity`**
```java
package com.queuedrop.entry.infrastructure.persistence;

@Entity
@Table(name = "entries")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class EntryJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false) private long dropId;
    @Column(nullable = false) private long userId;
    @Column(nullable = false) private int entrySeq;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private EntryStatus status;

    @Column(nullable = false) private Instant appliedAt;
    @Column(nullable = false) private Instant updatedAt;

    // 전체 필드 생성자
}
```
- 유니크 제약은 Flyway SQL 이 정의한다. `@Table(uniqueConstraints = ...)` 는 문서용으로만 의미가 있어서 생략한다 (`ddl-auto: validate`).

**`EntryJpaRepository`**: Spring Data. 없을 때만 넣는 native INSERT.
```java
interface EntryJpaRepository extends JpaRepository<EntryJpaEntity, Long> {

    /**
     * (drop_id, user_id) 충돌만 무시한다. entry_seq 충돌은 예외로 올린다.
     * @return 1 = 저장, 0 = 같은 사용자 응모가 이미 있음
     */
    @Modifying
    @Query(value = """
            INSERT INTO entries (drop_id, user_id, entry_seq, status, applied_at, updated_at)
            VALUES (:dropId, :userId, :entrySeq, :status, :appliedAt, now())
            ON CONFLICT (drop_id, user_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("dropId") long dropId, @Param("userId") long userId,
                       @Param("entrySeq") int entrySeq, @Param("status") String status,
                       @Param("appliedAt") Instant appliedAt);

    Optional<EntryJpaEntity> findByDropIdAndUserId(long dropId, long userId);

    List<EntryJpaEntity> findAllByUserIdOrderByAppliedAtDesc(long userId);

    long countByDropId(long dropId);        // 테스트·5단계용
}
```

**`EntryMapper`**
```java
final class EntryMapper {

    private EntryMapper() {}

    static Entry toDomain(EntryJpaEntity e) {
        return Entry.restore(e.getId(), e.getDropId(), e.getUserId(),
                e.getEntrySeq(), e.getStatus(), e.getAppliedAt());
    }

    // 저장은 native INSERT 로 필드를 직접 넘기므로 toEntity 는 5단계(상태 변경)에서 필요해지면 추가한다.
}
```

**`EntryRepositoryImpl`**
```java
@Repository
@RequiredArgsConstructor
class EntryRepositoryImpl implements EntryRepository {

    private final EntryJpaRepository jpa;

    @Override
    @Transactional
    public boolean saveIfAbsent(Entry entry) {
        int inserted = jpa.insertIfAbsent(entry.getDropId(), entry.getUserId(), entry.getEntrySeq(),
                entry.getStatus().name(), entry.getAppliedAt());
        return inserted == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return jpa.findByDropIdAndUserId(dropId, userId).map(EntryMapper::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Entry> findAllByUserId(long userId) {
        return jpa.findAllByUserIdOrderByAppliedAtDesc(userId).stream().map(EntryMapper::toDomain).toList();
    }
}
```

**"유니크 위반 무시" 를 이렇게 구현한 이유** (구현 결정)
| 방식 | 문제 |
|------|------|
| `jpa.save()` 후 `DataIntegrityViolationException` 잡기 | 같은 트랜잭션에서 제약 위반이 나면 Hibernate 세션을 더 쓸 수 없고, 트랜잭션이 rollback-only 가 된다. 예외를 잡아도 커밋 시 다시 터진다. 또 **어떤 제약**이 위반됐는지(user 인지 seq 인지) 구분하려면 제약 이름을 문자열로 파싱해야 한다 |
| `exists` 확인 후 `save` | 확인과 저장 사이에 같은 이벤트가 동시에 처리되면 둘 다 통과한다 (concurrency 3) |
| **`INSERT ... ON CONFLICT (drop_id, user_id) DO NOTHING`** | 예외 없이 "저장 안 됨(0)" 을 돌려준다. **충돌 대상을 `(drop_id, user_id)` 로만 지정**해서, `entry_seq` 충돌(정상이라면 일어나지 않는 버그 상황)은 `DataIntegrityViolationException` 으로 그대로 올라가 재시도 → DLT 로 드러난다 |

- 엔티티 분리 구조라 "도메인 모델 → 엔티티 → save" 경로가 필요 없다. 새 응모 저장은 native INSERT 하나로 끝난다.
- PostgreSQL 전용 문법이다. 다른 DB 로 바꿀 계획이 없으므로 받아들인다.

#### presentation

**`EntryCreatedListener`**
```java
package com.queuedrop.entry.presentation.listener;

@Component
@RequiredArgsConstructor
public class EntryCreatedListener {

    private final EntrySaveService entrySaveService;

    @KafkaListener(id = "entry-saver", topics = Topics.ENTRY_CREATED)
    public void on(EntryCreated message) {
        entrySaveService.save(new EntrySaveCommand(
                message.dropId(), message.userId(), message.entrySeq(), message.appliedAt()));
    }
}
```
- 예외를 잡지 않는다. DB 장애 등은 `DefaultErrorHandler` 가 재시도 → DLT 로 보낸다.
- 리스너 `id` 는 6장 유령 이벤트 테스트에서 컨테이너를 일시 정지(pause)하는 데 쓴다.
- 배치 리스너 + bulk insert 는 이 단계에서 하지 않는다. 8단계 부하 테스트에서 DB 쓰기가 병목으로 확인되면 적용한다.

---

### 3.4 조회

#### application

**`MyEntryResult`**
```java
package com.queuedrop.entry.application.dto;

/**
 * status 는 문자열로 내보낸다 (APPLIED / WON / LOST).
 * domain enum 을 그대로 넣으면 presentation 이 domain 타입에 의존하게 된다 (R4).
 * entrySeq, appliedAt 은 Redis 로 보완한 경우 null 이다.
 */
public record MyEntryResult(long dropId, Integer entrySeq, String status, Instant appliedAt) {

    public static MyEntryResult from(Entry entry) {
        return new MyEntryResult(entry.getDropId(), entry.getEntrySeq(),
                entry.getStatus().name(), entry.getAppliedAt());
    }

    public static MyEntryResult pendingSave(long dropId) {
        return new MyEntryResult(dropId, null, "APPLIED", null);
    }
}
```
- `status` 를 문자열로 두는 건 **(구현 결정)** 이다. application 전용 enum 을 따로 두는 방법도 있지만, 값이 domain enum 과 1:1 이라 중복만 늘어난다.

**`EntryQueryService`**
```java
@Service
@RequiredArgsConstructor
public class EntryQueryService {

    private final EntryRepository entryRepository;
    private final EntryGate entryGate;

    public List<MyEntryResult> myEntries(long userId) {
        return entryRepository.findAllByUserId(userId).stream().map(MyEntryResult::from).toList();
    }

    public MyEntryResult myEntry(long dropId, long userId) {
        return entryRepository.findByDropIdAndUserId(dropId, userId)
                .map(MyEntryResult::from)
                .orElseGet(() -> {
                    if (entryGate.isApplicant(dropId, userId)) {
                        return MyEntryResult.pendingSave(dropId);     // 판정은 됐고 DB 저장 대기 중
                    }
                    throw new EntryApplyException(EntryErrorCode.ENTRY_NOT_FOUND);
                });
    }
}
```
- **Redis 보완은 단건 조회에만 적용한다.** 목록 조회(`/me`)는 사용자 기준으로 Redis 를 찾으려면 모든 드로우의 applicants Set 을 훑어야 해서 보완하지 않는다. 방금 응모한 건이 몇 초 동안 목록에 안 보일 수 있다 (설계 수정 필요 ④).
- 응모 성공 응답(202)의 `entrySeq` 를 클라이언트가 이미 갖고 있으므로, 단건 보완 응답의 `entrySeq` 는 null 로 둔다.

#### presentation

```java
// EntryController 이어서

@GetMapping("/me")
public List<MyEntryResponse> myEntries(@RequestHeader(UserIdHeader.NAME) long userId) {
    return entryQueryService.myEntries(userId).stream().map(MyEntryResponse::from).toList();
}

@GetMapping("/drops/{dropId}/me")
public MyEntryResponse myEntry(@PathVariable long dropId,
                               @RequestHeader(UserIdHeader.NAME) long userId) {
    return MyEntryResponse.from(entryQueryService.myEntry(dropId, userId));
}

public record MyEntryResponse(long dropId, Integer entrySeq, String status, Instant appliedAt) {
    public static MyEntryResponse from(MyEntryResult r) {
        return new MyEntryResponse(r.dropId(), r.entrySeq(), r.status(), r.appliedAt());
    }
}
```

---

### 3.5 build.gradle (entry-service)
```groovy
dependencies {
    implementation project(':common')
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    runtimeOnly 'org.postgresql:postgresql'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    implementation 'org.springframework.boot:spring-boot-starter-kafka'
    // Flyway: 0단계에서 추가 (starter 이름은 Boot 4 기준 spring-boot-starter-flyway + flyway-database-postgresql, 구현 시 확인)

    testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
    testImplementation testFixtures(project(':common'))      // ArchUnit 규칙, Testcontainers 지원
    testImplementation 'org.awaitility:awaitility'            // Boot BOM 관리 버전 (구현 시 확인)
    // Testcontainers(postgresql, kafka, redis): 0단계에서 루트에 추가
}
```

---

## 4. 테스트 케이스

### 4.1 Fake 포트 (application 단위 테스트용)
| Fake | 구현 대상 | 동작 |
|------|-----------|------|
| `FakeEntryGate` | `EntryGate` | 드로우별 정원·응모자 Set·seq 를 메모리에 둔다. `nextOutcome(...)` 으로 다음 판정 결과를 강제할 수 있다. `compensateCalls()` 로 보상 호출 기록 조회 |
| `FakeEntryEventPublisher` | `EntryEventPublisher` | 발행된 payload 목록 저장. `failNext()` 로 다음 발행을 `EventPublishException` 으로 실패시킨다 |
| `InMemoryEntryRepository` | `EntryRepository` | `(dropId, userId)` 키 Map. `saveIfAbsent` 는 키가 있으면 false. **seq 중복이면 예외** (실제 DB 와 같은 의미) |
| `InMemoryDropSnapshotRepository` | `DropSnapshotRepository` | `dropId` 키 Map. `saveIfAbsent` 는 키가 있으면 false |
| `Clock.fixed(...)` | `Clock` | 고정 시각 |

### 4.2 테스트 목록
| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 |
|----|-----------|---------------|-------------------------------|------|
| T4-01 | domain 단위 | `EntryTest` | G: 정상 값 / W: `Entry.apply` / T: status=APPLIED, id=null. G: entrySeq=0 / T: `DomainException` | REQ-ENTRY-01 |
| T4-02 | domain 단위 | `DropSnapshotTest` | G: capacity < winner, winner=0, openAt ≥ closeAt / W: `register` / T: 각각 `DomainException`. 정상 값 → status=OPEN | REQ-DROP-02 |
| T4-03 | application 단위 | `EntryApplyServiceTest` | G: Gate 가 OK(seq=7) / W: apply / T: 결과 seq=7, 발행 payload 1건(dropId, userId, seq=7, appliedAt=고정 시각), 보상 호출 0 | REQ-ENTRY-01 |
| T4-04 | application 단위 | `EntryApplyServiceTest` (`@ParameterizedTest`) | G: Gate 가 CLOSED / NO_TOKEN / DUPLICATE / SOLD_OUT / W: apply / T: 같은 이름의 `EntryErrorCode` 예외, 발행 0, 보상 0 | REQ-ENTRY-02, 03 |
| T4-05 | application 단위 | `EntryApplyServiceTest` | G: Gate OK, Publisher `failNext()` / W: apply / T: `TEMPORARILY_FAILED`, 보상 호출 1 (같은 dropId, userId) | NFR-03 |
| T4-06 | application 단위 | `EntryApplyServiceTest` | G: 발행 실패 + 보상도 예외 / W: apply / T: 여전히 `TEMPORARILY_FAILED` (보상 예외가 원래 예외를 가리지 않음) | NFR-03 |
| T4-07 | application 단위 | `EntrySaveServiceTest` | G: Gate 응모자에 없음 / W: save / T: 저장소 0건 (유령 이벤트 무시) | NFR-03 |
| T4-08 | application 단위 | `EntrySaveServiceTest` | G: 응모자에 있음 / W: save 2번 / T: 저장소 1건, 예외 없음 | NFR-02 |
| T4-09 | application 단위 | `DropSnapshotRegisterServiceTest` | G: 이미 저장된 스냅샷 / W: register / T: 저장소 그대로, Gate.initialize 는 호출됨 (Redis 쪽 복구 기회). G: 잘못된 값 / T: `InvalidDropSnapshotException` | REQ-DROP-02 |
| T4-10 | application 단위 | `EntryQueryServiceTest` | G: DB 없음 + Gate 응모자 / T: status=APPLIED, entrySeq=null. G: 둘 다 없음 / T: `ENTRY_NOT_FOUND`. G: DB 있음 / T: DB 값 | REQ-ENTRY-06 |
| T4-11 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 초기화(정원 3), 토큰 있음 / W: tryApply / T: OK(seq=1), remaining=2, applicants 에 userId | REQ-ENTRY-01, 02 |
| T4-12 | infra 통합 (Redis) | `RedisEntryGateTest` | CLOSED 4가지: meta 없음 / state=CLOSED / now = openAt-1ms / now = closeAt (정각). 반대로 now = openAt (정각) → OK | REQ-ENTRY-02 |
| T4-13 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 토큰 키 없음 / T: NO_TOKEN, remaining 변화 없음 | REQ-QUEUE-06 |
| T4-14 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 이미 응모 / T: DUPLICATE, remaining·seq 변화 없음 | REQ-ENTRY-03 |
| T4-15 | infra 통합 (Redis) | `RedisEntryGateTest` | G: remaining=0 / T: SOLD_OUT, seq 변화 없음 | REQ-ENTRY-02 |
| T4-16 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 응모 1건 / W: compensate 2번 / T: remaining 은 한 번만 +1, applicants 에서 제거 | NFR-03 |
| T4-17 | infra 통합 (Redis) | `RedisEntryGateTest` | G: A 응모(seq=1) → 보상 / W: B 응모 / T: **B 의 seq=2** (번호 재사용 없음) | NFR-03 |
| T4-18 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 초기화(정원 100) 후 응모 30건 / W: initialize 다시 호출(정원 100) / T: remaining=70 유지, 반환 0 | REQ-DROP-02 |
| T4-19 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: (drop 1, user 1, seq 1) 저장 / W: 같은 (drop, user) 다른 seq 저장 / T: false, 예외 없음, 1건 | NFR-02 |
| T4-20 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: (drop 1, user 1, seq 1) / W: (drop 1, user 2, **seq 1**) 저장 / T: `DataIntegrityViolationException` (seq 충돌은 숨기지 않음) | NFR-03 |
| T4-21 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | 조회 매핑: 저장 → `findByDropIdAndUserId` 필드 일치, `findAllByUserId` 최신순 | REQ-ENTRY-06 |
| T4-22 | infra 통합 (PostgreSQL) | `DropSnapshotRepositoryImplTest` | 같은 dropId 2번 저장 → 두 번째 false, 첫 값 유지 | REQ-DROP-02 |
| T4-23 | infra 통합 (Kafka) | `KafkaEntryEventPublisherTest` | W: publishCreated / T: `entry.created` 에 key=`{dropId}:{userId}`, 메시지 필드 일치 (테스트 컨슈머로 수신) | NFR-01 |
| T4-24 | infra 통합 (Kafka) | `KafkaEntryEventPublisherTest` | G: Kafka 컨테이너 정지 / W: publishCreated / T: 4초 안에 `EventPublishException` | NFR-03 |
| T4-25 | presentation 슬라이스 | `EntryControllerTest` (`@WebMvcTest`) | 202 + `{"entrySeq":7}` / 에러 코드별 409·503·404 + code 값 / `X-User-Id` 없음 → 400 | REQ-ENTRY-01, 06 |
| T4-26 | presentation 통합 | `EntryCreatedListenerTest` | G: 응모자 등록 / W: 같은 `EntryCreated` 2번 발행 / T: Awaitility 로 DB 1건 유지 확인 | NFR-02 |
| T4-27 | 구조 | `ArchitectureTest` | R1~R7 통과. 특히 `presentation` 이 `domain` 을 import 하지 않음, `EntryApplyService` 시그니처에 domain 타입 없음 | 설계 |
| T4-28 | **시나리오 (동시성)** | `EntryConcurrencyTest` | G: 정원 100, 사용자 1,000명 모두 토큰 보유 / W: 동시 응모 / T: 성공 정확히 100, SOLD_OUT 900, Redis remaining=0, SCARD=100, **seq 100개 모두 다름**, 최종 DB 100건 | NFR-01 |
| T4-29 | **시나리오 (동시성)** | `EntryConcurrencyTest` | G: 정원 100, 사용자 1명 / W: 같은 사용자 10번 동시 응모 / T: 성공 1, DUPLICATE 9, remaining=99, 최종 DB 1건 | NFR-02 |
| T4-30 | 시나리오 (보상) | `EntryCompensationScenarioTest` | G: 발행 실패 Publisher 로 교체 / W: HTTP 응모 / T: 503 `ENTRY_TEMPORARILY_FAILED`, remaining·applicants 원상복구, 같은 사용자 재응모(정상 Publisher) → 202 | NFR-03 |
| T4-31 | **시나리오 (유령 이벤트)** | `GhostEventScenarioTest` | 4.4 참고. T: DB 에 A 1건(seq=1), B 1건(seq=2), Redis SCARD=2 == DB 2 | NFR-03 |
| T4-32 | 시나리오 (재수신) | `DropCreatedRedeliveryTest` | G: `drop.created` 처리 후 응모 30건 / W: 같은 `drop.created` 재발행 / T: remaining=70, drop_snapshots 1행 | REQ-DROP-02 |

### 4.3 동시성 테스트 스케치 (T4-28, T4-29)
application 서비스를 직접 호출한다. HTTP 를 거치면 톰캣 스레드 풀이 동시성을 제한해서 "동시" 요청이 줄어든다. HTTP 경로는 8단계 부하 테스트에서 본다.

```java
@SpringBootTest
class EntryConcurrencyTest extends IntegrationTestSupport {      // 0단계: Testcontainers 지원 클래스

    @Autowired EntryApplyService entryApplyService;
    @Autowired StringRedisTemplate redis;
    @Autowired EntryJpaRepository entryJpaRepository;
    @Autowired DropSnapshotRegisterService registerService;

    @Test
    void 정원_100에_1000명이_동시에_응모하면_정확히_100명만_성공한다() throws Exception {
        long dropId = 1L;
        int capacity = 100;
        int users = 1_000;
        givenOpenDrop(dropId, capacity);                          // registerService 로 초기화
        LongStream.rangeClosed(1, users).forEach(u -> givenToken(dropId, u));   // queue 토큰 키 직접 SET

        AtomicInteger success = new AtomicInteger();
        Map<EntryErrorCode, AtomicInteger> failures = new ConcurrentHashMap<>();
        Set<Integer> seqs = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(users);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {   // Java 21
            for (long u = 1; u <= users; u++) {
                long userId = u;
                pool.submit(() -> {
                    try {
                        start.await();                                     // 전원 동시에 출발
                        EntryApplyResult r = entryApplyService.apply(new EntryApplyCommand(dropId, userId));
                        success.incrementAndGet();
                        seqs.add(r.entrySeq());
                    } catch (EntryApplyException e) {
                        failures.computeIfAbsent((EntryErrorCode) e.getErrorCode(), k -> new AtomicInteger())
                                .incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(success).hasValue(capacity);
        assertThat(failures.get(EntryErrorCode.SOLD_OUT)).hasValue(users - capacity);
        assertThat(seqs).hasSize(capacity);
        assertThat(redis.opsForValue().get("entry:" + dropId + ":remaining")).isEqualTo("0");
        assertThat(redis.opsForSet().size("entry:" + dropId + ":applicants")).isEqualTo(capacity);

        await().atMost(Duration.ofSeconds(20))                             // 비동기 저장 대기
               .untilAsserted(() -> assertThat(entryJpaRepository.countByDropId(dropId)).isEqualTo(capacity));
    }

    @Test
    void 같은_사용자가_10번_동시에_응모하면_1번만_성공한다() throws Exception {
        // 같은 구조. users 루프 대신 같은 userId 로 10개 작업 제출.
        // T: success=1, DUPLICATE=9, remaining=99, 최종 DB 1건
    }
}
```
- 가상 스레드를 쓰는 이유: 고정 크기 풀(예: 64)에 1,000개 작업을 넣고 "전원 준비 완료" 래치를 기다리면, 64개만 실행돼서 래치가 영원히 풀리지 않는다. 가상 스레드는 작업마다 스레드가 생겨서 1,000개가 모두 `start.await()` 에서 대기한다.
- Redis 커넥션 풀(Lettuce 는 기본적으로 커넥션 하나를 공유)이 실제 동시성을 제한할 수 있다. 그래도 **Lua 원자성 검증**이 목적이라 충분하다. 요청 순서가 섞이기만 하면 된다.

### 4.4 유령 이벤트 시나리오 재현 (T4-31)
[sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 5.2 의 "브로커는 기록했는데 ack 를 못 받은" 상황은 실제 네트워크로 만들기 어렵다. 그래서 **실제로 발행한 뒤 실패를 던지는 테스트용 Publisher** 로 재현한다.

```java
/** 테스트 전용: 지정한 사용자의 첫 발행은 실제로 보낸 뒤 실패를 던진다 (= ack 유실). */
class AckLostEntryEventPublisher implements EntryEventPublisher {

    private final EntryEventPublisher delegate;               // 실제 KafkaEntryEventPublisher
    private final Set<Long> ackLostOnce = ConcurrentHashMap.newKeySet();

    void loseAckOnceFor(long userId) { ackLostOnce.add(userId); }

    @Override
    public void publishCreated(EntryCreatedPayload payload) {
        delegate.publishCreated(payload);                     // 브로커에는 기록됨
        if (ackLostOnce.remove(payload.userId())) {
            throw new EventPublishException("ack 유실 시뮬레이션", null);
        }
    }
}
```
`@TestConfiguration` 에서 `@Primary` 빈으로 등록한다.

**순서 (설계 5.2 해결 후 시퀀스와 같게)**
1. 드로우 초기화(정원 100), A·B 토큰 발급
2. **저장 리스너 일시 정지**: `registry.getListenerContainer("entry-saver").pause()`. 이렇게 해야 A 가 재응모하기 전에 유령 이벤트가 처리되는 걸 막고, 설계 그림의 순서를 그대로 재현할 수 있다.
3. `loseAckOnceFor(A)` → A 응모 → **503** (Kafka 에는 (A, seq 1) 기록, Redis 는 보상됨)
4. B 응모 → 202, seq 2
5. A 재응모 → 202, seq 3
6. 리스너 재개: `resume()`
7. Awaitility 로 확인
   - DB 2건: A 1건(**seq 1**, 유령 이벤트가 먼저 저장되고 seq 3 은 user 충돌로 무시), B 1건(seq 2)
   - Redis `SCARD applicants` = 2 == DB 건수 → 5단계 추첨이 대기 없이 진행 가능

**변형 케이스 (같은 테스트 클래스에 추가)**
- 2번(일시 정지)을 빼면 유령 이벤트가 A 재응모 전에 처리될 수 있다. 이때는 `isApplicant(A) == false` 라 무시되고, A 는 seq 3 으로 저장된다. **어느 순서든 A·B 각 1건, 합계 2건**이어야 한다.
- A 가 재응모하지 않는 경우: 유령 이벤트는 무시되고 DB 는 B 1건, Redis SCARD 1 → 일치.

---

## 5. 커밋 계획

| # | 커밋 메시지 | 포함 내용 |
|---|-------------|-----------|
| 1 | `feat(entry): 응모·드로우 스냅샷 테이블 마이그레이션 추가` | `V1__create_entry_tables.sql`, `ddl-auto: validate` |
| 2 | `feat(entry): 응모·드로우 스냅샷 도메인 모델 추가` | `Entry`, `EntryStatus`, `ApplyOutcome`, `DropSnapshot`, `SnapshotStatus`, Repository 인터페이스, T4-01·02 |
| 3 | `feat(entry): 응모 유스케이스와 포트 정의` | Command/Result, `EntryGate`, `EntryEventPublisher`, 에러 코드, `EntryApplyService`, Fake 포트, T4-03~06 |
| 4 | `feat(entry): Redis Lua 기반 응모 게이트 구현` | Lua 3종, `EntryScripts`, `RedisEntryGate`, `EntryRedisKeys`, T4-11~18 |
| 5 | `feat(entry): Kafka 응모 이벤트 발행 구현` | `KafkaEntryEventPublisher`, producer 설정, T4-23·24 |
| 6 | `feat(entry): 응모 API 추가` | `EntryController`(apply), Response, `EntryExceptionHandler`, T4-25(apply 부분) |
| 7 | `feat(entry): 드로우 생성 이벤트 수신 및 응모 초기화` | `DropSnapshotRegisterService`, JPA(스냅샷), `DropCreatedListener`, T4-09·22·32 |
| 8 | `feat(entry): 응모 이벤트 저장 컨슈머 구현` | `EntrySaveService`, JPA(응모), `EntryCreatedListener`, consumer 설정·에러 핸들러, T4-07·08·19~21·26 |
| 9 | `feat(entry): 내 응모 조회 API 추가` | `EntryQueryService`, 조회 API, T4-10·25(조회 부분) |
| 10 | `test(entry): 응모 동시성·보상·유령 이벤트 시나리오 테스트` | T4-28~31, `ArchitectureTest`(T4-27) 확인 |
| 11 | `docs: 4단계 구현 결과 반영` | 6장 "설계 수정 필요" 항목을 설계 문서에 반영, 플랜 진행 현황 갱신 |

커밋 본문 예시 (#4)
```
feat(entry): Redis Lua 기반 응모 게이트 구현

- entry-apply.lua: 마감·토큰·중복·잔여 검사 후 DECR + SADD + INCR seq 원자 실행
- entry-compensate.lua: SREM 성공 시에만 INCR remaining (중복 보상 안전, seq 유지)
- entry-initialize.lua: meta 가 없을 때만 초기화 (drop.created 재수신 대비)
- RedisEntryGate: EntryGate 포트 구현
```

> 11번은 순서상 마지막이지만, 6장의 설계 수정은 **해당 코드를 커밋하기 전에** 설계 문서부터 고치는 게 원칙이다 (플랜 1.1). 실제로는 #3 전에 설계 문서 수정 커밋을 먼저 넣는 걸 권장한다.

---

## 6. 결정 필요 · 리스크

### 6.1 이 단계에서 확정하는 것
| # | 항목 | 결정 |
|---|------|------|
| ① | 컨슈머 저장 전 Redis 응모자 확인 | **확정 (2026-11-16 사용자 승인)**. `EntrySaveService` 에서 `isApplicant` 확인 후 없으면 저장하지 않음 |
| ② | Kafka 동기 발행 정책 | **확정 (2026-11-16)**. 애플리케이션 재시도 없음, producer 내부 재시도 + `request.timeout.ms=1000`, `delivery.timeout.ms=3000`, `get` 대기 3.5초. 쿠폰(6단계)도 같은 정책 |
| ③ | 저장 컨슈머 실패 처리 | **확정 (2026-11-16)**. 3회 재시도 후 `{topic}.DLT` |

### 6.2 설계 수정 필요 (구현 전에 설계 문서부터 고침)
| # | 대상 문서 | 현재 | 변경 |
|---|-----------|------|------|
| ① | [entry-apply.md](../../design/05-features/entry-apply.md) 3장, [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D2, [implementation-plan.md](../implementation-plan.md) 4단계 | `get(3s)` + 재시도 2회 | 애플리케이션 재시도 없음. producer 내부 재시도(멱등) + `request.timeout.ms=1000`, `delivery.timeout.ms=3000`, `get` 대기 3.5초 → 설계 반영 완료 (2026-11-16) |
| ② | [architecture.md](../../design/02-architecture/architecture.md) 4장, [infrastructure.md](../../design/03-infrastructure/infrastructure.md) | DLT 없음 | `drop.created.DLT`, `entry.created.DLT`(+ 6단계 `coupon.issued.DLT`) 토픽 추가, 재시도 3회 후 DLT → 설계 반영 완료 (2026-11-16) |
| ③ | [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.1 | `EntryRepository.save(Entry) Entry`, `existsByDropIdAndUserId` | `saveIfAbsent(Entry) boolean`, `findByDropIdAndUserId`, `findAllByUserId` → 설계 반영 완료 (2026-11-16) |
| ④ | [entry-apply.md](../../design/05-features/entry-apply.md) 3장 API 설명 | "조회 API 는 DB 에 없으면 Redis 로 보완" | **단건 조회(`/drops/{dropId}/me`)만** 보완. 목록(`/me`)은 DB 만 → 설계 반영 완료 (2026-11-16) |
| ⑤ | [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 3장 | `DropCreated` 에 `productId` 없음 | [architecture.md](../../design/02-architecture/architecture.md) 4장 페이로드에는 있음 → 둘 중 하나로 통일 (entry 는 쓰지 않으므로 어느 쪽이든 무방, ranking·queue 확인 필요) → 설계 반영 완료 (2026-11-16) |
| ⑥ | [entry-apply.md](../../design/05-features/entry-apply.md) 3장, [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.5 | 에러 코드 5개 | `ENTRY_NOT_FOUND`(404), `MISSING_USER_HEADER`(400), `InvalidDropSnapshotException` 추가 → 설계 반영 완료 (2026-11-16) |

### 6.3 리스크
| 리스크 | 영향 | 대응 |
|--------|------|------|
| Kafka 지연이 응답 시간에 그대로 들어감 | 응모 p99 가 최대 3.5초까지 늘 수 있음 | 8단계 부하 테스트로 측정. `linger.ms`, 파티션 수 조정 |
| `TimeoutException` 후 뒤늦은 기록 (유령 이벤트) | 보상된 응모가 Kafka 에 남음 | 저장 전 `isApplicant` 확인 + seq 카운터 (T4-31 로 검증) |
| 보상 Lua 실패 | 정원 한 자리가 영구히 비어 있음 (언더 응모) | 로그 알림. 정합성 위반은 아님 (D2) |
| Redis 재시작 시 데이터 유실 | 오버 응모 가능 | 0단계의 AOF everysec. 최대 1초 유실 가능성은 남음 |
| Redis Cluster 미지원 | 키가 `{}` 해시태그 없이 여러 슬롯에 흩어져 Lua 다중 키 실행 불가 | 현재 단일 인스턴스 전제. 클러스터 전환 시 키를 `entry:{42}:meta` 형태의 해시태그로 바꾸고 queue 토큰 키도 같은 태그를 쓰게 해야 함 |
| 서버 시계 차이 | 인스턴스마다 `now` 가 달라 마감 경계에서 판정이 조금씩 다를 수 있음 | 판정은 Redis 한 곳에서 하므로 결과가 갈리진 않음. 서버 NTP 동기화 전제 |
| `drop.created` 전파 지연 | 오픈 직후 `CLOSED` 응답 | 드로우 등록 시 `openAt` 최소 1분 이후 검증 (2단계 확정 규칙) |
| queue 토큰 키 형식 의존 | queue 가 키 형식을 바꾸면 응모가 전부 `NO_TOKEN` | `EntryRedisKeys.queueToken` 주석 + 4단계 E2E(5단계)에서 실제 queue 와 함께 검증 |
| Spring Kafka 4 JSON 직렬화 클래스 변경 | 설정 오류로 기동 실패 | 구현 시 확인 (3.0 참고) |

---

## 7. 완료 체크리스트

### 플랜 완료 조건 ↔ 테스트
| 플랜 4단계 완료 조건 | 테스트 ID |
|----------------------|-----------|
| 통합(Lua): 결과 5가지가 각각 올바르게 나옴 | T4-11, T4-12, T4-13, T4-14, T4-15 |
| 동시성: 정원 100, 1,000명 동시 응모 → 성공 정확히 100, Redis 잔여 0, 최종 DB 100건 | T4-28 |
| 동시성: 같은 사용자 10번 동시 응모 → 성공 1 | T4-29 |
| 발행 실패 → 보상 후 잔여 정원·응모자 원상복구, 503 | T4-05, T4-16, T4-30 |
| 유령 이벤트 시나리오 → DB 에 A, B 모두 1건씩 | T4-17, T4-31 |
| 같은 이벤트 재전달 → DB 1건 유지 | T4-08, T4-19, T4-26 |
| `drop.created` 재수신 → 잔여 정원 덮어쓰기 없음 | T4-18, T4-22, T4-32 |

### 공통 완료 조건
- [ ] `./gradlew :entry-service:build` 통과 (T4-27 ArchUnit 포함)
- [ ] API 3개가 [entry-apply.md](../../design/05-features/entry-apply.md) API 표와 일치 (경로, 202, 에러 코드)
- [ ] 6.2 "설계 수정 필요" ①~⑥ 설계 문서 반영 + 변경 이력 추가
- [x] 6.1 결정 ① 을 플랜 4장 "미결정 사항" 표와 설계 README "확인 필요" 에서 확정으로 갱신 (2026-11-16)
- [ ] 플랜 진행 현황에서 4단계 ✅
