# 4단계 — 응모 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 발행 정책·DLT·저장 전 Redis 확인 확정 반영 (0단계 토픽 초기화에 DLT 추가됨), JSON 직렬화 클래스명을 0단계 공통 설정과 통일, 설계 반영 완료 표시 |
| v1.2 | 2026-11-16 | 0~3단계 실제 코드에 정렬. 에러 계약: `EntryErrorCode implements ErrorCode`(코드 `ENTRY_*`, 상태는 enum 이 보유), `ApplicationException` 으로 던지고 0단계 `GlobalExceptionHandler` 가 매핑 — `BusinessException`·`EntryApplyException`·`EntryExceptionHandler`·`UserIdHeader` 제거, 헤더 누락은 `COMMON_MISSING_USER_HEADER`(400). `DomainException` 은 entry `domain/exception` 에 서비스별로 둔다(queue 와 같은 형태). 잘못된 `drop.created` 는 queue 처럼 application 이 `DomainException` 을 잡아 로그·스킵(재시도·DLT 없음) — `InvalidDropSnapshotException` 제거, 설계 문서 반영. `DropSnapshotRegisterCommand` 를 `Long`/null 검증으로. Kafka 설정은 0단계 `application.yml` 공통 블록(이미 있음) 기준으로 추가분만 표기, `DefaultErrorHandler` 빈 자동 적용(3단계 확인), `@KafkaListener(idIsGroup = false)`. `entry:{dropId}:remaining` 정수 문자열 계약·queue 의존 명시. 스크립트 빈 `EntryScripts` → `infrastructure/config/RedisScriptConfig`, 테스트 지원은 `*ContainerConfig` `@Import`, 테스트 슬라이스 의존성(`data-redis-test`·`data-jpa-test`). 테스트 T4-33~35 추가. 커밋 계획을 테스트 포함 기능 커밋 단위로 재편. 6.1 에 E-34·E-39 사용자 결정 기록 |
| v1.3 | 2026-11-17 | 커밋 #3 검증 반영: `DropSnapshotRegisterService` 가 Redis 를 재수신 메시지 값이 아니라 DB 에 저장된 스냅샷 값으로 초기화 (DB·Redis 정원 불일치 방지). T4-09 에 인프라 예외 전파 케이스 추가 |
| v1.4 | 2026-11-17 | 커밋 #8 구현 확인: 역직렬화 실패 레코드를 원본 바이트로 DLT 에 보내도록 `byte[]` 전용 템플릿 추가(JSON 템플릿은 Base64 문자열로 감쌈). Spring Kafka 4 의 `DeadLetterPublishingRecoverer` 기본 목적지가 `{topic}-dlt` 라서 리졸버로 `{topic}.DLT`·같은 파티션을 명시 (토픽 이름·설계 문서는 그대로). 6단계 coupon 도 같은 리졸버 사용 |
| v1.5 | 2026-11-17 | 백로그 E-40·E-41 반영 (사용자 결정 2026-11-17, 6.1 ⑥⑦): 요청 ID = eventId 를 판정 전에 만들어 Gate 에 넘김. 새 키 `requests`(Hash)·`persisted`(Set), `entry-apply.lua` 에 `HSET requests`, 새 `entry-claim.lua`(저장 컨슈머가 `isApplicant` 대신 호출), `entry-compensate.lua` 3결과(`CompensationResult`). 발행 실패여도 ALREADY_PERSISTED 면 202. `RedisEntryGate` 가 Redis 예외를 `GateUnavailableException` 으로 감싸고 서비스는 같은 eventId 로 보상 후 503. 3.0·3.2·3.3 코드·Lua 전문, 4.1 Fake, 4.2 테스트 T4-36~42 추가·기존 테스트 기대값 변경 표시, 4.4 순서 B, 5장 커밋 #10a~c, 6장 결정·리스크 갱신 |
| v1.6 | 2026-11-17 | 6.3 리스크에 스크립트 캐시 미적재 시 판정 타임아웃 행 추가 (#10b Red 중 확인, 동작 변경 없음). 5장: T4-31 기대값 변경을 10c → 10b 로 이동 |

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
| `common.event`: `DropCreated(dropId, productId, title, entryCapacity, winnerCount, openAt, closeAt)`, `EntryCreated(eventId, dropId, userId, entrySeq, appliedAt)` (id 는 `Long`), `Topics.DROP_CREATED`·`ENTRY_CREATED`·`DROP_CREATED_DLT`·`ENTRY_CREATED_DLT` | 0 | Listener, Publisher |
| `common` testFixtures: `com.queuedrop.common.test.archunit.LayerRules`, `com.queuedrop.common.test.container.{Postgres,Redis,Kafka}ContainerConfig` (`@Import` 로 사용, `@ServiceConnection`) | 0 | `ArchitectureTest`(이미 있음), 통합 테스트 |
| entry-service `application.yml`(datasource·JPA `validate`·Flyway·Redis·Kafka 공통 블록), `build.gradle`(webmvc·jpa·flyway·redis·kafka, `kafka-test`·`webmvc-test`) | 0 | 3.0, 3.5 |
| `common.error`: `ErrorCode`(code·status·message), `ApplicationException`, `CommonErrorCode`(`MISSING_USER_HEADER` 등), `ErrorResponse` / entry `presentation/api/GlobalExceptionHandler`(+`GlobalExceptionHandlerTest`) | 0 | 에러 코드 → HTTP 매핑 (수정 없음) |
| 토픽 `drop.created`(1), `entry.created`(6), `drop.created.DLT`(1), `entry.created.DLT`(6) — `infra/kafka/create-topics.sh` | 0 | Kafka |
| Gateway 의 `X-User-Id` 헤더 주입 | 1 | Controller |
| `drop.created` 발행 | 2 | 드로우 정보 수신 |
| queue 의 입장 토큰 키 `queue:{dropId}:token:{userId}` (`QueueKeys.token`, 값 = 발급 시각 ms, TTL 300초) | 3 | 응모 Lua 의 토큰 검사 (`EXISTS` 만) |
| queue 가 읽는 entry 키 `entry:{dropId}:remaining` (`QueueKeys.entryRemaining`, `admit.lua`·`RedisRemainingCapacityReader`) | 3 | 이 단계가 **쓰는** 키. 형식 계약은 3.0 |

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
├── build.gradle                                        # 테스트 슬라이스 의존성 추가 (3.5)
└── src
    ├── main
    │   ├── java/com/queuedrop/entry
    │   │   ├── presentation
    │   │   │   ├── api
    │   │   │   │   ├── EntryController.java
    │   │   │   │   ├── GlobalExceptionHandler.java     # 0단계에서 생성됨 (수정 없음)
    │   │   │   │   └── response
    │   │   │   │       ├── EntryApplyResponse.java
    │   │   │   │       └── MyEntryResponse.java
    │   │   │   └── listener
    │   │   │       ├── DropCreatedListener.java
    │   │   │       └── EntryCreatedListener.java
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
    │   │   │   │   ├── EventPublishException.java
    │   │   │   │   ├── CompensationResult.java                # v1.5 (E-40)
    │   │   │   │   └── GateUnavailableException.java          # v1.5 (E-41)
    │   │   │   └── exception
    │   │   │       └── EntryErrorCode.java             # common.error.ErrorCode 구현 (v1.2)
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
    │   │   │       └── DomainException.java            # 서비스별로 둔다 (queue 와 같은 형태, v1.2)
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
    │   │       │   └── RedisEntryGate.java
    │   │       ├── messaging
    │   │       │   └── KafkaEntryEventPublisher.java
    │   │       └── config
    │   │           ├── ClockConfig.java
    │   │           ├── RedisScriptConfig.java          # Lua 스크립트 빈 (queue 와 같은 위치·이름, v1.2)
    │   │           └── KafkaConsumerConfig.java        # 에러 핸들러, DLT
    │   └── resources
    │       ├── application.yml                         # 0단계에서 생성됨. 3.0 의 추가분만 반영
    │       ├── scripts
    │       │   ├── entry-initialize.lua
    │       │   ├── entry-apply.lua
    │       │   ├── entry-compensate.lua
    │       │   └── entry-claim.lua                         # v1.5 (E-40)
    │       └── db/migration
    │           └── V1__create_entry_tables.sql         # .gitkeep 대체
    └── test/java/com/queuedrop/entry
        ├── ArchitectureTest.java                       # 0단계에서 생성됨 (수정 없음)
        ├── ApplicationContextTest.java                 # 0단계에서 생성됨 (V1 적용 후에도 통과해야 함)
        ├── domain/model
        │   ├── EntryTest.java
        │   └── DropSnapshotTest.java
        ├── application/service
        │   ├── fake
        │   │   ├── FakeEntryGate.java
        │   │   ├── FakeEntryEventPublisher.java
        │   │   ├── InMemoryEntryRepository.java
        │   │   └── InMemoryDropSnapshotRepository.java
        │   ├── EntryApplyServiceTest.java
        │   ├── EntrySaveServiceTest.java
        │   ├── DropSnapshotRegisterServiceTest.java
        │   └── EntryQueryServiceTest.java
        ├── application/exception/EntryErrorCodeTest.java
        ├── infrastructure
        │   ├── redis/RedisEntryGateTest.java
        │   ├── persistence/EntryRepositoryImplTest.java
        │   ├── persistence/DropSnapshotRepositoryImplTest.java
        │   └── messaging/KafkaEntryEventPublisherTest.java
        ├── presentation
        │   ├── api/EntryControllerTest.java
        │   ├── api/GlobalExceptionHandlerTest.java     # 0단계에서 생성됨 (수정 없음)
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
| `entry:{dropId}:requests` | Hash (field=userId, value=eventId) | 쓰기 (v1.5) |
| `entry:{dropId}:persisted` | Set (userId) | 쓰기 (v1.5) |
| `queue:{dropId}:token:{userId}` | String(TTL) | **읽기 전용** (queue 소유) |

```java
package com.queuedrop.entry.infrastructure.redis;

/** entry-service Redis 키 이름의 단일 출처 (architecture.md 5장). queue-service 의 QueueKeys 와 같은 형태 */
public final class EntryRedisKeys {

    private EntryRedisKeys() {}

    public static String meta(Long dropId)       { return "entry:" + dropId + ":meta"; }
    /** queue-service 가 읽는다 (QueueKeys.entryRemaining). 값은 항상 10진 정수 문자열 */
    public static String remaining(Long dropId)  { return "entry:" + dropId + ":remaining"; }
    public static String applicants(Long dropId) { return "entry:" + dropId + ":applicants"; }
    public static String seq(Long dropId)        { return "entry:" + dropId + ":seq"; }
    /** userId → 이 자리를 잡은 요청의 eventId (v1.5, E-40) */
    public static String requests(Long dropId)   { return "entry:" + dropId + ":requests"; }
    /** 저장 컨슈머가 DB 저장 전에 확정한 userId (v1.5, E-40) */
    public static String persisted(Long dropId)  { return "entry:" + dropId + ":persisted"; }

    /** queue-service 소유 키 — 읽기 전용. QueueKeys.token(dropId, userId) 과 같은 문자열이어야 한다 */
    public static String queueToken(Long dropId, Long userId) {
        return "queue:" + dropId + ":token:" + userId;
    }
}
```
- `requests`·`persisted` 의 TTL 은 다른 응모 키와 함께 5단계에서 정한다 (E-42). 이 단계에서는 만료를 걸지 않는다.
- 키 문자열의 `{dropId}` 는 설계 문서의 자리표시자다. 실제 키에는 중괄호 없이 `entry:42:meta` 처럼 들어간다. 해시 태그는 쓰지 않는다 (6.1 ⑤, E-39).
- queue 토큰 키 형식은 이 클래스에 중복으로 정의한다 **(구현 결정)**. `common` 으로 옮기면 서비스 간 결합이 코드로 드러나는 장점이 있지만, 키 하나 때문에 common 에 Redis 개념이 들어가는 게 더 크다고 판단했다. queue 쪽도 같은 이유로 `QueueKeys.entryRemaining` 을 중복 정의했다 (3단계).

**서비스 간 키 계약** (v1.2, 3단계 코드 기준)
| 키 | 쓰는 쪽 | 읽는 쪽 | 계약 |
|----|---------|---------|------|
| `queue:{dropId}:token:{userId}` | queue `admit.lua` (`SET ... EX 300`, 값 = 발급 시각 ms) | entry `entry-apply.lua` (`EXISTS`) | 키 이름만 의존. 값은 읽지 않는다 |
| `entry:{dropId}:remaining` | entry `entry-initialize.lua`(`SET capacity`)·`entry-apply.lua`(`DECR`)·`entry-compensate.lua`(`INCR`, COMPENSATED 일 때만) | queue `admit.lua`(`tonumber`)·`RedisRemainingCapacityReader`(`Long.parseLong`) | **10진 정수 문자열**. `SET`/`DECR`/`INCR` 만 쓰므로 항상 만족. 키가 없으면 queue 는 진입 허용·입장 보류 (3단계 결정) → `drop.created` 처리 전에는 아무도 입장하지 않는다 |
- 3단계 테스트는 이 키를 `EntryRemainingFixture` 로 직접 세팅했다. 이 단계에서 실제 쓰기 경로가 생기므로, 형식이 바뀌면 queue 의 `admit.lua`·`RedisRemainingCapacityReader` 가 깨진다. T4-11·18 에서 `GET remaining` 이 정수 문자열인지 함께 확인한다.

#### 시각 주입 (`infrastructure/config/ClockConfig`)
```java
@Configuration
public class ClockConfig {          // queue·product 와 같은 형태
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
```
- application 서비스는 `Clock` 을 주입받아 `now` 를 만든다. 테스트에서는 `Clock.fixed(...)` 로 바꾼다.
- domain 은 `Clock` 도 모른다. 시각은 항상 인자로 받는다.

#### application.yml (0단계 공통 블록 + 이 단계 추가분)
0단계에서 만든 `application.yml` 에 datasource·JPA(`validate`)·Flyway·Redis·Kafka(producer 동기 발행 정책, consumer `ErrorHandlingDeserializer`) 가 이미 있다 (queue-service 와 같은 Kafka 블록). 이 단계는 아래 `# v1.2 추가` 표시 줄만 넣는다.
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
      ddl-auto: validate        # 스키마는 Flyway 가 만든다. 엔티티와 불일치 시 기동 실패
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: false
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      timeout: 1s                # v1.2 추가: 명령 타임아웃 (Lettuce 기본 60초). 응모 응답 경로가 Redis 장애에 오래 묶이지 않게
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer   # Spring Kafka 4 (Jackson 3)
      acks: all
      retries: 2147483647          # 횟수가 아니라 delivery.timeout.ms 로 전체 시간을 제한
      properties:
        enable.idempotence: true
        request.timeout.ms: 1000
        delivery.timeout.ms: 3000
        max.block.ms: 1000         # send() 호출 자체의 블로킹 상한 (기본 60초)
        linger.ms: 0               # v1.2 추가: Kafka 4.0 클라이언트 기본값이 5ms 로 바뀌었다. 응답 경로에 배치 대기를 넣지 않는다
    consumer:
      group-id: ${spring.application.name}
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
        spring.json.trusted.packages: com.queuedrop.common.event
    listener:                      # v1.2 추가
      ack-mode: record             # 레코드 단위로 처리 성공 후 커밋
      concurrency: 3               # entry.created 파티션 6 → 인스턴스 2개 기준 3씩. drop.created(파티션 1)는 1개만 일함

entry:                             # v1.2 추가
  publish:
    await-timeout: 3500ms          # delivery.timeout.ms(3000) + 여유
```
- `enable-auto-commit` 은 적지 않는다. Spring Kafka 가 명시하지 않으면 `false` 로 두고 컨테이너가 커밋한다 (queue 와 같음).
- JSON 직렬화 클래스는 0단계에서 Spring Kafka 4 의 `JacksonJsonSerializer` / `JacksonJsonDeserializer` 로 확정됐다 (v1.1 의 "구현 시 확인" 해소).

**Producer 설정 근거**
| 설정 | 값 | 이유 |
|------|----|------|
| `acks` | `all` | 리더와 ISR 모두 기록해야 성공. "성공 응답 = 이벤트 기록" 불변식 ([D2](../../design/04-non-functional/concurrency-consistency.md)) |
| `enable.idempotence` | `true` | producer 내부 재시도로 생기는 중복 기록을 브로커가 제거 |
| `retries` | `MAX_INT` | 재시도 횟수가 아니라 `delivery.timeout.ms` 로 전체 시간을 제한 |
| `request.timeout.ms` / `delivery.timeout.ms` | 1000 / 3000 | send 결과가 **3초 안에 반드시 확정**되게 한다. `get(timeout)` 만 걸면 future 는 타임아웃돼도 producer 는 최대 120초(기본값) 동안 계속 재시도해서, 보상 후에 뒤늦게 기록되는 유령 이벤트가 생기기 쉽다 |
| `max.block.ms` | 1000 | 메타데이터 조회 등으로 `send()` 호출 자체가 막히는 시간 상한. 최악 응답 시간 = 1초 + `get` 3.5초 |
| `linger.ms` | 0 | 응답 지연에 배치 대기 시간을 넣지 않는다. 8단계 부하 테스트에서 재조정 |

> **설계와 다른 점 (설계 수정 필요 ①, 반영 완료)**: 설계는 "`get(3s)` + 애플리케이션 재시도 2회" 였다. **애플리케이션 재시도를 없애고 producer 내부 재시도(멱등) + `delivery.timeout.ms=3000`** 으로 바꿨다. 애플리케이션이 같은 레코드를 다시 send 하면 멱등 producer 도 중복을 막지 못하고(별개 레코드로 취급), 3초 × 3회면 응답이 최대 9초까지 늘어난다.

**Consumer 설정 근거**
| 설정 | 값 | 이유 |
|------|----|------|
| `enable-auto-commit` | `false` (Spring Kafka 기본) | 자동 커밋은 처리 전에 오프셋이 커밋될 수 있다 → 저장 전 장애 시 유실 |
| `ack-mode` | `record` | 레코드 하나를 **DB 저장 성공 후** 커밋. at-least-once + 컨슈머 멱등으로 정확히 한 번처럼 동작 |
| `ErrorHandlingDeserializer` | - | 역직렬화 실패 레코드 하나가 파티션 전체를 막지 않게 한다 (0단계 공통) |
| 타입 정보 | `__TypeId__` 헤더 (JSON serializer 기본) | 모든 서비스가 같은 `common` 클래스를 쓰므로 헤더의 FQCN 이 그대로 맞는다. `trusted.packages` 로 범위를 제한 |
| group-id | `${spring.application.name}` = `entry-service` | 두 리스너가 같은 그룹을 쓴다. `@KafkaListener(id = ...)` 를 주면 기본(`idIsGroup = true`)으로 id 가 group-id 를 덮어쓰므로 **`idIsGroup = false`** 를 함께 준다 (v1.2) |

#### Kafka 에러 핸들러 (`infrastructure/config/KafkaConsumerConfig`) (구현 결정)
```java
package com.queuedrop.entry.infrastructure.config;

/**
 * DB 에 저장하는 컨슈머 실패 정책 (architecture.md 4장, 6.1 ③): 1초 간격 3회 재시도 후 {topic}.DLT.
 * Spring Boot 가 이 CommonErrorHandler 빈을 기본 리스너 컨테이너 팩토리에 적용한다 (3단계 queue 에서 확인).
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 DLT 로 간다.
 */
@Configuration
public class KafkaConsumerConfig {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Spring Kafka 4 기본 목적지는 {topic}-dlt 라서 리졸버로 {topic}.DLT·같은 파티션 번호를 지정한다 (v1.4)
        // 값 타입 순서 매칭: 역직렬화 실패 레코드(byte[])는 ByteArraySerializer 템플릿으로 원본 그대로 (v1.4)
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);      // 기존 producer 설정 복사 + ByteArraySerializer, destroy() 에서 닫음
        templates.put(Object.class, kafkaTemplate);
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }
}
```
- queue 의 `KafkaConsumerConfig` 와 같은 형태에 recoverer 만 더한다. 저장 실패(DB 장애 등)는 1초 간격 3번 재시도 후 `entry.created.DLT` / `drop.created.DLT` 로 보낸다.
- DLT 토픽은 0단계 `create-topics.sh` 가 원본과 같은 파티션 수로 만든다 (리졸버가 원본 파티션 번호로 보내므로). 토픽 이름은 `Topics.*_DLT` 상수.
- 잘못된 `drop.created`(도메인 불변식 위반)는 application 이 로그·스킵하므로 이 핸들러까지 오지 않는다 (3.1, v1.2). DLT 로 가는 건 인프라 장애와 역직렬화 실패뿐이다. 예외로, `entry.created` 의 `dropId`·`userId` 가 null 이면 리스너의 언박싱 NPE 가 재시도 후 DLT 로 간다 (같은 서비스가 발행하는 메시지라 실제로는 생기지 않으며, 생기면 DLT 에서 드러나는 쪽이 맞다).
- 역직렬화 실패 레코드는 값이 원본 `byte[]` 로 복원돼 recoverer 에 전달된다. JSON 템플릿으로 보내면 Base64 JSON 문자열로 바뀌는 것을 확인해서(v1.4, T4-34 poison pill 케이스), `byte[]` 용 템플릿을 먼저 매칭하는 Map 생성자를 쓴다.
- DLT 로 간 응모는 Redis 에는 있고 DB 에는 없는 상태라, 5단계 추첨의 건수 비교에서 불일치로 드러난다. 그때 DLT 를 다시 처리하면 된다.

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

**`DomainException`** (`domain/exception`): 도메인 불변식 위반. 0단계 `common` 에는 없고 서비스별로 둔다 (queue-service `domain/exception/DomainException` 과 같은 형태, v1.2).
```java
package com.queuedrop.entry.domain.exception;

/** 도메인 불변식 위반. application 이 받아 처리한다 (메시지는 로그에만 남는다). */
public class DomainException extends RuntimeException {
    public DomainException(String message) {
        super(message);
    }
}
```

**`DropSnapshot`**: product 드로우의 사본. 생성 시 불변식을 검증한다.
```java
package com.queuedrop.entry.domain.model;

public final class DropSnapshot {

    private final Long dropId;
    private final int entryCapacity;
    private final int winnerCount;
    private final Instant openAt;
    private final Instant closeAt;
    private final SnapshotStatus status;

    private DropSnapshot(Long dropId, int entryCapacity, int winnerCount,
                         Instant openAt, Instant closeAt, SnapshotStatus status) {
        // 필드 대입
    }

    /** drop.created 수신 시 생성. 항상 OPEN 으로 시작한다. */
    public static DropSnapshot register(Long dropId, int entryCapacity, int winnerCount,
                                        Instant openAt, Instant closeAt) {
        if (dropId == null || openAt == null || closeAt == null) {
            throw new DomainException("필수 값 누락");          // 메시지 필드 누락도 스킵 대상 (v1.2)
        }
        if (winnerCount < 1 || entryCapacity < winnerCount) {
            throw new DomainException("entryCapacity >= winnerCount >= 1 이어야 합니다");
        }
        if (!openAt.isBefore(closeAt)) {
            throw new DomainException("openAt < closeAt 이어야 합니다");
        }
        return new DropSnapshot(dropId, entryCapacity, winnerCount, openAt, closeAt, SnapshotStatus.OPEN);
    }

    /** 저장소에서 복원할 때만 사용 (Mapper 전용). 검증하지 않는다. */
    public static DropSnapshot restore(Long dropId, int entryCapacity, int winnerCount,
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

/** dropId 는 Long: 메시지의 null 을 리스너에서 언박싱하다 NPE(→ 재시도·DLT)가 나지 않고 도메인 검증까지 오게 한다 (v1.2) */
public record DropSnapshotRegisterCommand(
        Long dropId, int entryCapacity, int winnerCount, Instant openAt, Instant closeAt) {}
```

**`DropSnapshotRegisterService`**: DB 저장 후 Redis 초기화. 둘 다 멱등이라 어느 단계에서 실패해도 재전달로 복구된다.
```java
package com.queuedrop.entry.application.service;

/** drop.created 처리. 잘못된 메시지는 로그 후 스킵한다 (queue 의 QueueMetaRegisterService 와 같은 방식, v1.2) */
@Slf4j
@Service
@RequiredArgsConstructor
public class DropSnapshotRegisterService {

    private final DropSnapshotRepository dropSnapshotRepository;
    private final EntryGate entryGate;

    public void register(DropSnapshotRegisterCommand command) {
        DropSnapshot snapshot;
        try {
            snapshot = DropSnapshot.register(
                    command.dropId(), command.entryCapacity(), command.winnerCount(),
                    command.openAt(), command.closeAt());
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", command.dropId(), e.getMessage());
            return;
        }

        DropSnapshot stored = dropSnapshotRepository.saveIfAbsent(snapshot)   // ① DB (자체 트랜잭션)
                ? snapshot
                : dropSnapshotRepository.findById(snapshot.getDropId()).orElseThrow();

        entryGate.initialize(stored.getDropId(), stored.getEntryCapacity(),   // ② Redis (NX)
                stored.getOpenAt(), stored.getCloseAt());
    }
}
```
- **Redis 는 DB 에 저장된 스냅샷 값으로 초기화한다 (v1.3)**. 재수신 메시지의 값이 처음과 다르더라도(DB 저장 후 Redis 초기화가 실패한 뒤 값이 바뀐 메시지가 오는 경우) DB 와 Redis 정원이 갈라지지 않게 DB 를 기준으로 삼는다.
- **순서와 실패 처리** (구현 결정)
  | 실패 지점 | 상태 | 재전달 시 |
  |-----------|------|-----------|
  | ① DB 저장 | 아무것도 없음 | ①부터 다시 |
  | ② Redis 초기화 | DB 만 있음 | ① 은 이미 있어서 건너뜀, ② 실행 |
- 메서드에 `@Transactional` 을 걸지 않는다. Redis 호출을 DB 트랜잭션 안에 두면, 커밋 실패 시 Redis 만 초기화된 상태가 남는다.
- 도메인 검증 실패(`DomainException`)는 재시도해도 같은 결과라서 **application 이 잡아 warn 로그 후 스킵**한다 (v1.2). 재시도·DLT 로 보내지 않는다. 잘못된 드로우는 Redis 에 초기화되지 않으므로 응모는 `CLOSED` 로 막히고, product 쪽 데이터를 고쳐 다시 발행하면 된다.
- `try` 는 도메인 생성만 감싼다. DB·Redis 장애 예외는 그대로 올려 `DefaultErrorHandler` 가 재시도 → `drop.created.DLT` 로 보내게 한다.

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

@Component
@RequiredArgsConstructor
public class DropCreatedListener {

    private final DropSnapshotRegisterService registerService;

    @KafkaListener(id = "entry-drop-created", idIsGroup = false, topics = Topics.DROP_CREATED)
    public void on(DropCreated message) {
        registerService.register(new DropSnapshotRegisterCommand(
                message.dropId(), message.entryCapacity(), message.winnerCount(),
                message.openAt(), message.closeAt()));
    }
}
```
- 리스너는 예외를 잡지 않는다 (v1.2). 잘못된 값은 application 이 스킵하고, DB·Redis 장애 예외만 올라와 `DefaultErrorHandler` 가 재시도 → DLT 로 보낸다.
- v1.1 의 `InvalidDropSnapshotException`(application 예외로 바꿔 던지고 리스너가 잡음)은 제거했다. queue 와 같은 방식으로 맞추고, presentation 이 domain 예외를 볼 필요(R4)도 원래 없게 된다.

---

### 3.2 응모

```
POST /api/entries/drops/{dropId}
  → EntryController → EntryApplyService           (eventId = UUID 를 판정 전에 생성, v1.5)
      ├─ EntryGate.tryApply        (Redis Lua: 마감·토큰·중복·잔여 검사 + DECR + SADD + HSET requests + INCR seq)
      │    └─ GateUnavailableException → EntryGate.compensate(같은 eventId) → 503
      ├─ EntryEventPublisher       (Kafka entry.created, ack 대기)
      └─ 실패 시 EntryGate.compensate (Redis Lua: 3결과)
           ├─ ALREADY_PERSISTED → 202 (컨슈머가 이미 저장 확정)
           └─ COMPENSATED / NOT_OURS → 503
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

/**
 * Redis 기반 선착순 판정 게이트. 판정·보상·저장 확정·초기화를 원자적으로 수행한다.
 * tryApply·compensate·claimForSave 는 Redis 장애(타임아웃 포함) 시 GateUnavailableException 을 던진다 (v1.5).
 */
public interface EntryGate {

    /** OK 면 requests[userId] = eventId 를 함께 기록한다. eventId 는 호출자가 판정 전에 만든 요청 ID */
    GateResult tryApply(long dropId, long userId, String eventId, Instant now);

    /**
     * eventId 요청의 판정을 되돌린다. seq 는 되돌리지 않는다. 두 번 호출해도 안전하다 (두 번째는 NOT_OURS).
     * @return ALREADY_PERSISTED: 저장 컨슈머가 이미 확정 — 되돌리지 않음
     *         NOT_OURS: requests[userId] ≠ eventId (다른 요청의 응모 또는 판정 미실행) — 되돌리지 않음
     *         COMPENSATED: 응모자·요청 제거 + 정원 복구
     */
    CompensationResult compensate(long dropId, long userId, String eventId);

    /**
     * 저장 컨슈머가 DB 저장 전에 호출한다. true 를 반환한 뒤에는 보상이 거절된다 (persisted).
     * @return false: 보상된 응모이거나 이전 시도의 유령 이벤트 — 저장하지 않는다. 재전달에도 true (멱등)
     */
    boolean claimForSave(long dropId, long userId, String eventId);

    /** 단건 조회의 저장 대기 보완용 (EntryQueryService). 저장 경로에서는 쓰지 않는다 (v1.5) */
    boolean isApplicant(long dropId, long userId);

    /** 응모 메타·정원 초기화. 이미 초기화돼 있으면 아무것도 하지 않는다. */
    void initialize(long dropId, int capacity, Instant openAt, Instant closeAt);
}

/** 보상 Lua 결과 (v1.5, E-40) */
public enum CompensationResult { COMPENSATED, ALREADY_PERSISTED, NOT_OURS }

/** Redis 호출 실패 (연결 실패·명령 타임아웃). 타임아웃이면 Lua 가 실행됐는지 알 수 없다 (v1.5, E-41) */
public class GateUnavailableException extends RuntimeException {
    public GateUnavailableException(String message, Throwable cause) { super(message, cause); }
}

/** entrySeq 는 outcome == OK 일 때만 값이 있다. */
public record GateResult(ApplyOutcome outcome, Integer entrySeq) {
    public static GateResult ok(int entrySeq)                 { return new GateResult(ApplyOutcome.OK, entrySeq); }
    public static GateResult rejected(ApplyOutcome outcome)   { return new GateResult(outcome, null); }
    public boolean isOk()                                     { return outcome == ApplyOutcome.OK; }
}
```
- `GateUnavailableException` 은 포트 예외다. application 이 Spring Data 의 `DataAccessException` 을 알지 않도록 어댑터가 감싼다 (`EventPublishException` 과 같은 방식).
- `isApplicant`·`initialize` 는 감싸지 않는다(v1.5 범위 밖). 조회·드로우 수신 경로의 Redis 장애는 기존처럼 전파된다.
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

**에러 코드** (`application/exception`) — 0단계 에러 계약에 정렬 (v1.2)
```java
package com.queuedrop.entry.application.exception;

import com.queuedrop.common.error.ErrorCode;

/** 응답 code 는 {SERVICE}_{REASON}, 상수명은 사유만 (layered-architecture.md 4.5). 상태 코드는 enum 이 갖는다 */
public enum EntryErrorCode implements ErrorCode {
    CLOSED("ENTRY_CLOSED", 409, "응모 기간이 아닙니다."),
    NO_TOKEN("ENTRY_NO_TOKEN", 409, "입장 토큰이 없거나 만료되었습니다."),
    DUPLICATE("ENTRY_DUPLICATE", 409, "이미 응모한 드로우입니다."),
    SOLD_OUT("ENTRY_SOLD_OUT", 409, "응모 정원이 마감되었습니다."),
    TEMPORARILY_FAILED("ENTRY_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요."),
    NOT_FOUND("ENTRY_NOT_FOUND", 404, "응모 내역이 없습니다.");      // (구현 결정) 단건 조회용

    private final String code;
    private final int status;
    private final String message;

    EntryErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }

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
```
- 예외는 `throw new ApplicationException(EntryErrorCode.X)`. 별도 `EntryApplyException`·`EntryExceptionHandler` 는 두지 않는다 — 0단계 `presentation/api/GlobalExceptionHandler` 가 `ErrorCode.status()`·`code()` 로 매핑한다 (queue 3단계 v1.2 와 같음).
- `from(ApplyOutcome)` 은 public static 이지만 application.service 의 메서드가 아니라 R6 대상이 아니다. presentation 은 `EntryErrorCode` 를 쓰지 않는다.

> **ApplyOutcome → 예외 변환 위치**: `EntryApplyService` 가 `EntryErrorCode.from(outcome)` 으로 변환해 `ApplicationException` 을 던진다. `ApplyOutcome` 은 application 밖으로 나가지 않는다.

**`EntryApplyService`**: 요청 ID 생성 → 판정 → 발행 → (실패 시) 보상 (v1.5).
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
        String eventId = UUID.randomUUID().toString();      // 요청 ID = eventId. 판정 전에 만든다 (E-40)

        GateResult gate;
        try {
            gate = entryGate.tryApply(command.dropId(), command.userId(), eventId, now);
        } catch (GateUnavailableException e) {
            // Lua 실행 여부를 모름 (E-41): 실행됐다면 되돌리고(COMPENSATED), 아니면 NOT_OURS 로 무변화
            compensateQuietly(command, eventId);
            throw new ApplicationException(EntryErrorCode.TEMPORARILY_FAILED, e);
        }
        if (!gate.isOk()) {
            throw new ApplicationException(EntryErrorCode.from(gate.outcome()));
        }

        EntryCreatedPayload payload = new EntryCreatedPayload(
                eventId, command.dropId(), command.userId(), gate.entrySeq(), now);

        try {
            eventPublisher.publishCreated(payload);
        } catch (EventPublishException e) {
            Optional<CompensationResult> result = compensateQuietly(command, eventId);
            if (result.filter(r -> r == CompensationResult.ALREADY_PERSISTED).isPresent()) {
                // 컨슈머가 이미 저장 확정 = 실제로 기록됨. 불변식("성공 = 기록")과 맞으므로 성공 (E-40)
                log.warn("발행 실패로 판단했으나 이미 저장됨 dropId={} userId={} eventId={}",
                        command.dropId(), command.userId(), eventId);
                return new EntryApplyResult(gate.entrySeq());
            }
            throw new ApplicationException(EntryErrorCode.TEMPORARILY_FAILED, e);   // 503
        }

        return new EntryApplyResult(gate.entrySeq());
    }

    /** @return 보상 결과. 보상 자체가 실패하면 empty (로그만 남기고 호출자는 503) */
    private Optional<CompensationResult> compensateQuietly(EntryApplyCommand command, String eventId) {
        try {
            return Optional.of(entryGate.compensate(command.dropId(), command.userId(), eventId));
        } catch (RuntimeException e) {
            // 보상 실패: 정원 한 자리가 새는 쪽(언더 응모)이라 정원 초과는 아니다 (D2). 알림 대상.
            // tryApply 타임아웃 경로에서는 Redis 에만 응모가 남아 D6 불일치가 된다 (6.3 리스크)
            log.error("보상 실패 dropId={} userId={} eventId={}",
                    command.dropId(), command.userId(), eventId, e);
            return Optional.empty();
        }
    }
}
```
- 응답 분기 (v1.5)

  | 상황 | 보상 결과 | 응답 |
  |------|-----------|------|
  | 발행 성공 | — | 202 |
  | 발행 실패 | `ALREADY_PERSISTED` | **202** (판정 때 받은 entrySeq) |
  | 발행 실패 | `COMPENSATED` / `NOT_OURS` / 보상 예외 | 503 `ENTRY_TEMPORARILY_FAILED` |
  | `tryApply` 가 `GateUnavailableException` | 결과와 무관 (보상 예외 포함) | 503 `ENTRY_TEMPORARILY_FAILED` |
- 발행 실패 경로의 `ALREADY_PERSISTED` 는 반드시 이 요청의 저장이다: `persisted` 에 들어간 사용자는 보상되지 않아 `applicants` 에 계속 남으므로, 그 뒤의 판정은 `DUPLICATE` 가 된다. 판정이 OK 였다면 그 전에는 `persisted` 에 없었다.
- `tryApply` 타임아웃 경로에서 `ALREADY_PERSISTED` 가 나오는 경우는 이전 요청이 이미 저장된 사용자뿐이다(이번 판정은 DUPLICATE 였을 것). entrySeq 를 모르므로 503 으로 답하고, 재시도하면 409 `ENTRY_DUPLICATE` 를 받는다.
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
-- KEYS[6] = entry:{dropId}:requests           (userId → eventId, v1.5)
-- ARGV[1] = userId
-- ARGV[2] = now (epoch ms, 애플리케이션 시각)
-- ARGV[3] = eventId (요청 ID, 애플리케이션이 판정 전에 생성, v1.5)
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

-- 5. 차감 + 응모자 등록 + 이 자리를 잡은 요청 기록 (보상·저장 확정이 eventId 로 대조)
redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[6], ARGV[1], ARGV[3])

-- 6. 응모 번호: 잔여 정원과 무관한 카운터 (증가만, 보상 시에도 되돌리지 않음)
local seq = redis.call('INCR', KEYS[4])
return { 'OK', seq }
```
| 구분 | 값 |
|------|-----|
| KEYS | `meta`, `remaining`, `applicants`, `seq`, `queueToken`, `requests` |
| ARGV | `userId`, `now(ms)`, `eventId` |
| 반환 | `['OK', seq]` / `['CLOSED']` / `['NO_TOKEN']` / `['DUPLICATE']` / `['SOLD_OUT']` |

- 모든 명령이 O(1) 이다. 반복문을 넣지 않는다 (Lua 실행 중에는 Redis 전체가 멈춘다).
- 초기화 전 드로우(`meta` 없음)를 `CLOSED` 로 처리하는 건 **(구현 결정)** 이다. 에러 코드를 늘리지 않고 "지금은 응모할 수 없음" 으로 묶었다. `drop.created` 전파가 늦은 경우가 여기에 해당한다.
- `closeAt` 정각은 `CLOSED` 다 (`now >= closeAt`). 설계의 `openAt ≤ now < closeAt` 과 같다.
- v1.5: 판정 순서는 그대로다. 거절 4종은 `requests` 를 건드리지 않는다. 불변식: `requests` 에 userId 가 있으면 `applicants` 에도 있다 (둘은 apply 에서 함께 추가, compensate 에서 함께 제거).

**`entry-compensate.lua` (전문, v1.5)**
```lua
-- 응모 판정 되돌리기 (Kafka 발행 최종 실패, 또는 판정 Lua 응답 타임아웃 시)
-- KEYS[1] = entry:{dropId}:remaining
-- KEYS[2] = entry:{dropId}:applicants
-- KEYS[3] = entry:{dropId}:requests
-- KEYS[4] = entry:{dropId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = eventId (되돌릴 요청)
-- return  : 1 = COMPENSATED, 0 = NOT_OURS, 2 = ALREADY_PERSISTED

-- 1. 저장 컨슈머가 이미 확정했으면 되돌리지 않는다 (DB 저장 ⇔ Redis 응모자 유지)
if redis.call('SISMEMBER', KEYS[4], ARGV[1]) == 1 then
    return 2
end

-- 2. 이 요청이 잡은 자리가 아니면 건드리지 않는다 (다른 요청의 응모, 판정 미실행, 이미 보상됨)
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
    return 0
end

-- 3. 되돌림 (seq 는 되돌리지 않는다)
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('INCR', KEYS[1])
return 1
```
| 구분 | 값 |
|------|-----|
| KEYS | `remaining`, `applicants`, `requests`, `persisted` |
| ARGV | `userId`, `eventId` |
| 반환 | `1` COMPENSATED / `0` NOT_OURS / `2` ALREADY_PERSISTED |

- `HGET` 은 필드가 없으면 Lua 에서 `false` 라 `~= eventId` 가 참 → `0`. 따라서 **두 번 호출해도 정원이 두 번 늘지 않는다** (두 번째는 `HDEL` 뒤라 NOT_OURS).
- 판정 Lua 가 실행되지 않은 채 호출돼도(E-41 타임아웃 경로) `requests` 에 이 eventId 가 없어 아무것도 바뀌지 않는다.
- `seq` 는 건드리지 않는다 (번호 재사용 방지, [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 5.2).

**`entry-claim.lua` (전문, v1.5 신규)**: 저장 컨슈머가 DB 저장 **전에** 호출한다. 보상 Lua 와 같은 Redis 에서 원자적으로 경합한다.
```lua
-- 저장 확정 (entry.created 저장 컨슈머, DB INSERT 전)
-- KEYS[1] = entry:{dropId}:applicants
-- KEYS[2] = entry:{dropId}:requests
-- KEYS[3] = entry:{dropId}:persisted
-- ARGV[1] = userId
-- ARGV[2] = eventId (메시지의 eventId)
-- return  : 1 = 저장 확정 (재전달에도 1), 0 = 저장하지 않음 (유령 이벤트)

-- 1. 보상된 응모의 유령 이벤트
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
    return 0
end

-- 2. 보상 후 재응모한 사용자의 이전 시도 유령 이벤트
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 0
end

-- 3. 확정: 이후 보상은 ALREADY_PERSISTED 로 거절된다
redis.call('SADD', KEYS[3], ARGV[1])
return 1
```
| 구분 | 값 |
|------|-----|
| KEYS | `applicants`, `requests`, `persisted` |
| ARGV | `userId`, `eventId` |
| 반환 | `1` 저장 / `0` 스킵 |

- **정합성 논증** ([D2](../../design/04-non-functional/concurrency-consistency.md) v1.3): claim 이 먼저면 보상은 `ALREADY_PERSISTED`(DB 저장 + Redis 응모자 유지 + 202), 보상이 먼저면 claim 은 `0`(DB 미저장 + Redis 응모자 없음 + 503). 어느 순서든 "DB 저장 ⇔ Redis 응모자 유지"라 풀린 자리를 다른 사용자가 가져가도 DB 응모 수가 정원을 넘지 않는다.
- 남는 불일치는 claim 후 DB INSERT 가 실패해 DLT 로 간 경우(Redis 응모자 있음, DB 없음)뿐이다. DLT 재처리 시 claim 은 다시 1 을 반환한다.

**`RedisScriptConfig`** (`infrastructure/config`, v1.2: queue 와 같은 위치·이름): 스크립트를 빈으로 한 번만 로드한다. `RedisTemplate.execute(script, ...)` 는 EVALSHA 를 먼저 시도하고, 스크립트 캐시가 없으면 EVAL 로 다시 보낸다.
```java
package com.queuedrop.entry.infrastructure.config;

/** Spring Data Redis 는 EVALSHA 를 먼저 시도하고 NOSCRIPT 면 EVAL 로 재시도한다 */
@Configuration
public class RedisScriptConfig {

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

    @Bean
    RedisScript<Long> entryClaimScript() {                    // v1.5
        return RedisScript.of(new ClassPathResource("scripts/entry-claim.lua"), Long.class);
    }
}
```

**`RedisEntryGate`**: `EntryGate` 구현.
```java
package com.queuedrop.entry.infrastructure.redis;

@Component
@RequiredArgsConstructor
public class RedisEntryGate implements EntryGate {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> entryInitializeScript;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> entryApplyScript;
    private final RedisScript<Long> entryCompensateScript;
    private final RedisScript<Long> entryClaimScript;

    @Override
    public GateResult tryApply(long dropId, long userId, String eventId, Instant now) {
        List<?> result = call(() -> redis.execute(entryApplyScript,
                List.of(EntryRedisKeys.meta(dropId), EntryRedisKeys.remaining(dropId),
                        EntryRedisKeys.applicants(dropId), EntryRedisKeys.seq(dropId),
                        EntryRedisKeys.queueToken(dropId, userId), EntryRedisKeys.requests(dropId)),
                String.valueOf(userId), String.valueOf(now.toEpochMilli()), eventId));

        ApplyOutcome outcome = ApplyOutcome.valueOf((String) result.get(0));
        if (outcome != ApplyOutcome.OK) {
            return GateResult.rejected(outcome);
        }
        return GateResult.ok(Math.toIntExact((Long) result.get(1)));
    }

    @Override
    public CompensationResult compensate(long dropId, long userId, String eventId) {
        Long result = call(() -> redis.execute(entryCompensateScript,
                List.of(EntryRedisKeys.remaining(dropId), EntryRedisKeys.applicants(dropId),
                        EntryRedisKeys.requests(dropId), EntryRedisKeys.persisted(dropId)),
                String.valueOf(userId), eventId));
        return switch (result.intValue()) {
            case 1 -> CompensationResult.COMPENSATED;
            case 0 -> CompensationResult.NOT_OURS;
            case 2 -> CompensationResult.ALREADY_PERSISTED;
            default -> throw new IllegalStateException("알 수 없는 보상 결과: " + result);
        };
    }

    @Override
    public boolean claimForSave(long dropId, long userId, String eventId) {
        Long result = call(() -> redis.execute(entryClaimScript,
                List.of(EntryRedisKeys.applicants(dropId), EntryRedisKeys.requests(dropId),
                        EntryRedisKeys.persisted(dropId)),
                String.valueOf(userId), eventId));
        return result == 1L;
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

    /** Redis 예외(연결 실패·명령 타임아웃 = QueryTimeoutException 등 DataAccessException 계열)를 포트 예외로 (v1.5, E-41) */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new GateUnavailableException("Redis 응모 게이트 호출 실패", e);
        }
    }
}
```
- v1.5: `tryApply`·`compensate`·`claimForSave` 만 `call()` 로 감싼다. 명령 타임아웃은 `spring.data.redis.timeout: 1s` (3.0). Lettuce 는 한 연결에서 명령 순서를 보장하므로, 타임아웃 뒤 보낸 보상 Lua 는 늦게 도착한 판정 Lua **뒤에** 실행된다 (E-41 근거, Lettuce 공유 연결 기준).
- `claimForSave` 의 `GateUnavailableException` 은 리스너까지 전파돼 `DefaultErrorHandler` 재시도 → DLT 로 간다 (DB 장애와 같은 경로).
- Lua 가 반환한 배열에서 문자열은 Java `String`(StringRedisTemplate 직렬화), 정수는 `Long` 으로 들어온다. queue `admit.lua`(`RedisScript<List>`, userId 문자열 배열)가 같은 방식으로 동작한다 (3단계 확인). 문자열·정수 혼합 배열은 이 단계가 처음이라 T4-11 에서 타입을 확인한다.
- 빈 이름 주입: 같은 타입(`RedisScript<Long>`) 빈이 셋이라 필드 이름(`entryInitializeScript`, `entryCompensateScript`, `entryClaimScript`)으로 구분된다. queue 의 `RedisWaitingQueueRepository` 와 같은 방식.

**`KafkaEntryEventPublisher`**: `EntryEventPublisher` 구현. application DTO 를 `common` 메시지로 바꿔 보낸다.
```java
package com.queuedrop.entry.infrastructure.messaging;

@Component
public class KafkaEntryEventPublisher implements EntryEventPublisher {

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
        } catch (RuntimeException e) {          // send() 가 동기로 던지는 경우 (max.block.ms 초과 등)
            throw new EventPublishException("entry.created 발행 실패", e);
        }
    }
}
```
- `send()` 가 동기로 던지는 예외는 Apache `KafkaException` 계열인지 Spring `KafkaException` 으로 감싸지는지 버전에 따라 다르다. 그래서 `RuntimeException` 으로 넓게 잡는다 (v1.2). 포트 밖으로 Kafka 예외가 새면 `EntryApplyService` 의 보상이 실행되지 않는다.
- `get()` 의 대기 시간(3.5초)은 producer 의 `delivery.timeout.ms`(3초)보다 조금 길다. 그래서 대부분 producer 가 먼저 성공/실패를 확정하고, `TimeoutException` 은 producer 가 응답하지 않는 예외 상황에서만 난다.
- `TimeoutException` 이어도 레코드가 나중에 기록될 수 있다. 그 경우가 5.2 의 유령 이벤트고, 저장 컨슈머의 claim Lua 와 3결과 보상, `seq` 카운터가 처리한다 (v1.5).

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

    @PostMapping("/drops/{dropId:\\d+}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EntryApplyResponse apply(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return EntryApplyResponse.from(entryApplyService.apply(new EntryApplyCommand(dropId, userId)));
    }

    // 조회 API 는 3.4
}

public record EntryApplyResponse(int entrySeq) {
    public static EntryApplyResponse from(EntryApplyResult result) {
        return new EntryApplyResponse(result.entrySeq());
    }
}
```
- 헤더 이름은 queue·product 컨트롤러처럼 리터럴 `"X-User-Id"` 로 쓴다. v1.1 의 `UserIdHeader` 상수 클래스는 제거 (v1.2).
- 경로 변수는 `{dropId:\d+}` 정규식으로 받는다 (queue 와 같음). 숫자가 아니면 404 `COMMON_NOT_FOUND`.

**에러 응답**: 0단계 `GlobalExceptionHandler` 를 그대로 쓴다 (수정 없음, v1.2). 별도 `@RestControllerAdvice` 를 두지 않는다.
| 원인 | HTTP | 응답 `code` |
|------|------|-------------|
| `ApplicationException(EntryErrorCode.CLOSED)` | 409 | `ENTRY_CLOSED` |
| `ApplicationException(EntryErrorCode.NO_TOKEN)` | 409 | `ENTRY_NO_TOKEN` |
| `ApplicationException(EntryErrorCode.DUPLICATE)` | 409 | `ENTRY_DUPLICATE` |
| `ApplicationException(EntryErrorCode.SOLD_OUT)` | 409 | `ENTRY_SOLD_OUT` |
| `ApplicationException(EntryErrorCode.TEMPORARILY_FAILED)` | 503 | `ENTRY_TEMPORARILY_FAILED` (발행 실패 또는 Redis 장애, v1.5) |
| `ApplicationException(EntryErrorCode.NOT_FOUND)` | 404 | `ENTRY_NOT_FOUND` **(구현 결정)** |
| `X-User-Id` 누락 | 400 | `COMMON_MISSING_USER_HEADER` (0단계 공통) |
| 숫자가 아닌 `X-User-Id` | 400 | `COMMON_INVALID_INPUT` |
- 응답 `message` 는 `ErrorCode.message()` 고정 문구다. 상태 코드는 `EntryErrorCode` 가 갖고, 핸들러는 `ErrorCode.status()` 만 본다.
- `NO_TOKEN` 은 403 으로 볼 수도 있지만, 클라이언트 처리가 "대기열로 돌아가기" 라서 다른 409 사유와 함께 묶었다 **(구현 결정)**.
- 표는 [entry-apply.md](../../design/05-features/entry-apply.md) 3장 에러 코드 표(v1.4)와 같다.

---

### 3.3 저장

```
entry.created → EntryCreatedListener → EntrySaveService
                                         ├─ EntryGate.claimForSave (Redis Lua: 유령 이벤트 거르기 + SADD persisted, v1.5)
                                         └─ EntryRepository.saveIfAbsent
```

#### application

**`EntrySaveCommand`**
```java
/** eventId: 메시지의 eventId = 응모 요청 ID (v1.5) */
public record EntrySaveCommand(String eventId, long dropId, long userId, int entrySeq, Instant appliedAt) {}
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
        // ① 저장 확정 (v1.5): 보상된 응모 또는 이전 시도의 유령 이벤트면 false. true 이후 보상은 거절된다
        if (!entryGate.claimForSave(command.dropId(), command.userId(), command.eventId())) {
            log.warn("유령 응모 이벤트 무시 dropId={} userId={} seq={} eventId={}",
                    command.dropId(), command.userId(), command.entrySeq(), command.eventId());
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
- **저장 전 Redis 확인은 이 단계에서 확정한다** (6장 결정 ①). v1.5 에서 `isApplicant`(check-then-act) 를 claim Lua 로 바꿨다 — 소비가 보상보다 먼저 오면 `isApplicant` 는 통과한 뒤 보상이 자리를 풀어 DB 응모 수 = 정원 + 1 이 될 수 있었다 (E-40, 6.1 ⑥).
- claim 이 true 를 반환한 뒤 `saveIfAbsent` 가 실패하면 예외가 전파돼 재시도 → DLT. 재처리 시 claim 은 다시 true 다.
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

    @KafkaListener(id = "entry-saver", idIsGroup = false, topics = Topics.ENTRY_CREATED)
    public void on(EntryCreated message) {
        entrySaveService.save(new EntrySaveCommand(message.eventId(),
                message.dropId(), message.userId(), message.entrySeq(), message.appliedAt()));
    }
}
```
- v1.5: `message.eventId()` 를 넘긴다. 0단계 `EntryCreated` 에 이미 있는 필드라 메시지 형식 변경은 없다.
- 예외를 잡지 않는다. DB 장애 등은 `DefaultErrorHandler` 가 재시도 → DLT 로 보낸다.
- 리스너 `id` 는 4.4 유령 이벤트 테스트에서 컨테이너를 일시 정지(pause)하는 데 쓴다. `idIsGroup = false` 로 group-id 는 `entry-service` 그대로 둔다.
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
                    throw new ApplicationException(EntryErrorCode.NOT_FOUND);
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
public List<MyEntryResponse> myEntries(@RequestHeader("X-User-Id") Long userId) {
    return entryQueryService.myEntries(userId).stream().map(MyEntryResponse::from).toList();
}

@GetMapping("/drops/{dropId:\\d+}/me")
public MyEntryResponse myEntry(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
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
0단계에서 만든 파일에 테스트 슬라이스 두 줄만 추가한다 (v1.2). Boot 4 는 슬라이스 어노테이션이 기술별 테스트 모듈로 나뉘어 있다 (queue·product 와 같음).
```groovy
dependencies {
    implementation project(':common')
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    runtimeOnly 'org.postgresql:postgresql'
    implementation 'org.springframework.boot:spring-boot-starter-flyway'
    implementation 'org.flywaydb:flyway-database-postgresql'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    implementation 'org.springframework.boot:spring-boot-starter-kafka'
    testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
    testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'      // @WebMvcTest (0단계)
    testImplementation 'org.springframework.boot:spring-boot-starter-data-redis-test'  // @DataRedisTest (v1.2 추가)
    testImplementation 'org.springframework.boot:spring-boot-starter-data-jpa-test'    // @DataJpaTest (v1.2 추가)
}
```
- `testFixtures(project(':common'))`(ArchUnit `LayerRules`, `*ContainerConfig`)와 `spring-boot-starter-test` 는 루트 `build.gradle` 이 4계층 서비스에 일괄 추가한다 (0단계).
- Awaitility 는 따로 추가하지 않는다. queue-service 가 같은 의존성 구성에서 `org.awaitility.Awaitility` 를 쓰고 있다.
- 슬라이스 패키지: `org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest`, `org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest`(+ `org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase(replace = NONE)`), `org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest`.

---

## 4. 테스트 케이스

### 4.1 Fake 포트 (application 단위 테스트용)
| Fake | 구현 대상 | 동작 |
|------|-----------|------|
| `FakeEntryGate` | `EntryGate` | 드로우별 정원·응모자 Set·seq 를 메모리에 둔다. `nextOutcome(...)` 으로 다음 판정 결과를 강제할 수 있다. `compensateCalls()` 로 보상 호출 기록 조회. **v1.5**: Lua 와 같은 의미로 `requests`(userId → eventId)·`persisted` 를 메모리에 두고 `compensate` 는 3결과, `claimForSave` 는 Lua 와 같은 판정. `tryApplyCalls()`·`compensateCalls()` 는 eventId 까지 기록. `nextCompensation(result)` 로 보상 결과 강제, `timeoutAfterApplyNext()`(상태를 바꾼 뒤 `GateUnavailableException`) / `timeoutBeforeApplyNext()`(상태 변화 없이 던짐), `failCompensateNext()` |
| `FakeEntryEventPublisher` | `EntryEventPublisher` | 발행된 payload 목록 저장. `failNext()` 로 다음 발행을 `EventPublishException` 으로 실패시킨다 |
| `InMemoryEntryRepository` | `EntryRepository` | `(dropId, userId)` 키 Map. `saveIfAbsent` 는 키가 있으면 false. **seq 중복이면 예외** (실제 DB 와 같은 의미) |
| `InMemoryDropSnapshotRepository` | `DropSnapshotRepository` | `dropId` 키 Map. `saveIfAbsent` 는 키가 있으면 false |
| `Clock.fixed(...)` | `Clock` | 고정 시각 |

### 4.2 테스트 목록
> 통합 테스트 설정 (v1.2, 0~3단계 방식): 컨테이너는 `common` testFixtures 의 `PostgresContainerConfig`·`RedisContainerConfig`·`KafkaContainerConfig` 를 `@Import` 한다 (공통 상위 클래스 없음).
> - Redis: `@DataRedisTest` + `@Import({RedisContainerConfig.class, RedisScriptConfig.class, RedisEntryGate.class})` (queue `RedisWaitingQueueRepositoryTest` 와 같음). `@BeforeEach` 에서 `FLUSHALL`.
> - PostgreSQL: `@DataJpaTest` + `@AutoConfigureTestDatabase(replace = NONE)` + `@Import({PostgresContainerConfig.class, EntryRepositoryImpl.class, ...})` (product `DropRepositoryImplTest` 와 같음).
> - Kafka·시나리오: `@SpringBootTest` + 필요한 `*ContainerConfig` `@Import`. 토픽은 테스트 `@TestConfiguration` 의 `NewTopic` 빈으로 리스너 기동 전에 만든다 (queue `DropCreatedListenerIntegrationTest` 와 같음, DLT 토픽 포함).
> - 컨트롤러: `@WebMvcTest(EntryController.class)` + `@Import(GlobalExceptionHandler.class)` + `@MockitoBean` 서비스 (queue `QueueControllerTest` 와 같음).

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 |
|----|-----------|---------------|-------------------------------|------|
| T4-01 | domain 단위 | `EntryTest` | G: 정상 값 / W: `Entry.apply` / T: status=APPLIED, id=null. G: entrySeq=0 / T: `DomainException` | REQ-ENTRY-01 |
| T4-02 | domain 단위 | `DropSnapshotTest` | G: capacity < winner, winner=0, openAt ≥ closeAt, dropId·openAt·closeAt null(v1.2) / W: `register` / T: 각각 `DomainException`. 정상 값 → status=OPEN | REQ-DROP-02 |
| T4-03 | application 단위 | `EntryApplyServiceTest` | G: Gate 가 OK(seq=7) / W: apply / T: 결과 seq=7, 발행 payload 1건(dropId, userId, seq=7, appliedAt=고정 시각), 보상 호출 0. **v1.5**: payload.eventId == tryApply 에 넘긴 eventId (UUID 형식) | REQ-ENTRY-01 |
| T4-04 | application 단위 | `EntryApplyServiceTest` (`@ParameterizedTest`) | G: Gate 가 CLOSED / NO_TOKEN / DUPLICATE / SOLD_OUT / W: apply / T: `ApplicationException` 의 `errorCode()` 가 같은 이름의 `EntryErrorCode`, 발행 0, 보상 0 | REQ-ENTRY-02, 03 |
| T4-05 | application 단위 | `EntryApplyServiceTest` | G: Gate OK, Publisher `failNext()` / W: apply / T: `TEMPORARILY_FAILED`, 보상 호출 1 (같은 dropId, userId, **v1.5: 같은 eventId**, 결과 COMPENSATED) | NFR-03 |
| T4-06 | application 단위 | `EntryApplyServiceTest` | G: 발행 실패 + 보상도 예외 / W: apply / T: 여전히 `TEMPORARILY_FAILED` (보상 예외가 원래 예외를 가리지 않음) | NFR-03 |
| T4-07 | application 단위 | `EntrySaveServiceTest` | G: Gate 응모자에 없음 / W: save / T: 저장소 0건 (유령 이벤트 무시). **v1.5**: `claimForSave` 가 false 인 두 경우(응모자 없음 / requests 의 eventId 다름) 모두 0건, claim 에 command.eventId 전달 | NFR-03 |
| T4-08 | application 단위 | `EntrySaveServiceTest` | G: 응모자에 있음 (v1.5: 같은 eventId 로 판정됨) / W: save 2번 / T: 저장소 1건, 예외 없음 | NFR-02 |
| T4-09 | application 단위 | `DropSnapshotRegisterServiceTest` | G: 이미 저장된 스냅샷 / W: register / T: 저장소 그대로, Gate.initialize 는 **저장된 스냅샷 값**으로 호출됨 (Redis 쪽 복구 기회, v1.3). G: 저장소·Gate 예외 / T: 예외 전파(스킵하지 않음), 저장 실패 시 initialize 0회. G: 잘못된 값(capacity < winner, null 필드) / W: register / T: **예외 없이 스킵** — 저장소 0건, Gate.initialize 호출 0 (v1.2, queue `QueueMetaRegisterServiceTest` 와 같은 방식) | REQ-DROP-02 |
| T4-10 | application 단위 | `EntryQueryServiceTest` | G: DB 없음 + Gate 응모자 / T: status=APPLIED, entrySeq=null. G: 둘 다 없음 / T: `ApplicationException(EntryErrorCode.NOT_FOUND)`. G: DB 있음 / T: DB 값 | REQ-ENTRY-06 |
| T4-11 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 초기화(정원 3), 토큰 있음 / W: tryApply / T: OK(seq=1, Java 타입 `Long` → int), `GET remaining` = `"2"`(10진 정수 문자열, queue 계약), applicants 에 userId | REQ-ENTRY-01, 02 |
| T4-12 | infra 통합 (Redis) | `RedisEntryGateTest` | CLOSED 4가지: meta 없음 / state=CLOSED / now = openAt-1ms / now = closeAt (정각). 반대로 now = openAt (정각) → OK | REQ-ENTRY-02 |
| T4-13 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 토큰 키 없음 / T: NO_TOKEN, remaining 변화 없음 | REQ-QUEUE-06 |
| T4-14 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 이미 응모 / T: DUPLICATE, remaining·seq 변화 없음 | REQ-ENTRY-03 |
| T4-15 | infra 통합 (Redis) | `RedisEntryGateTest` | G: remaining=0 / T: SOLD_OUT, seq 변화 없음 | REQ-ENTRY-02 |
| T4-16 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 응모 1건 / W: compensate 2번 / T: remaining 은 한 번만 +1, applicants 에서 제거. **v1.5**: 같은 eventId 로 호출, 1회차 COMPENSATED·2회차 NOT_OURS, requests 에서도 제거 | NFR-03 |
| T4-17 | infra 통합 (Redis) | `RedisEntryGateTest` | G: A 응모(seq=1) → 보상 / W: B 응모 / T: **B 의 seq=2** (번호 재사용 없음) | NFR-03 |
| T4-18 | infra 통합 (Redis) | `RedisEntryGateTest` | G: 초기화(정원 100) 후 응모 30건 / W: initialize 다시 호출(정원 100) / T: remaining=70 유지, 반환 0 | REQ-DROP-02 |
| T4-19 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: (drop 1, user 1, seq 1) 저장 / W: 같은 (drop, user) 다른 seq 저장 / T: false, 예외 없음, 1건 | NFR-02 |
| T4-20 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: (drop 1, user 1, seq 1) / W: (drop 1, user 2, **seq 1**) 저장 / T: `DataIntegrityViolationException` (seq 충돌은 숨기지 않음) | NFR-03 |
| T4-21 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | 조회 매핑: 저장 → `findByDropIdAndUserId` 필드 일치, `findAllByUserId` 최신순 | REQ-ENTRY-06 |
| T4-22 | infra 통합 (PostgreSQL) | `DropSnapshotRepositoryImplTest` | 같은 dropId 2번 저장 → 두 번째 false, 첫 값 유지 | REQ-DROP-02 |
| T4-23 | infra 통합 (Kafka) | `KafkaEntryEventPublisherTest` | W: publishCreated / T: `entry.created` 에 key=`{dropId}:{userId}`, 메시지 필드 일치 (테스트 컨슈머로 수신) | NFR-01 |
| T4-24 | infra 통합 (Kafka) | `KafkaEntryEventPublisherTest` | G: Kafka 컨테이너 정지 / W: publishCreated / T: 5초 안에 `EventPublishException` (최악 `max.block.ms` 1초 + `get` 3.5초) | NFR-03 |
| T4-25 | presentation 슬라이스 | `EntryControllerTest` (`@WebMvcTest`) | 202 + `{"entrySeq":7}` / `ApplicationException(EntryErrorCode.*)` 각 코드 → 409·503·404 + `code`(`ENTRY_*`)·고정 `message` / `X-User-Id` 없음 → 400 `COMMON_MISSING_USER_HEADER` / 숫자 아닌 dropId → 404 | REQ-ENTRY-01, 06 |
| T4-26 | presentation 통합 | `EntryCreatedListenerTest` | G: 응모자 등록 (v1.5: `tryApply` 로 같은 eventId 를 requests 에 기록) / W: 같은 `EntryCreated` 2번 발행 / T: Awaitility 로 DB 1건 유지 확인, persisted 에 userId | NFR-02 |
| T4-27 | 구조 | `ArchitectureTest` | R1~R7 통과. 특히 `presentation` 이 `domain` 을 import 하지 않음, `EntryApplyService` 시그니처에 domain 타입 없음 | 설계 |
| T4-28 | **시나리오 (동시성)** | `EntryConcurrencyTest` | G: 정원 100, 사용자 1,000명 모두 토큰 보유 / W: 동시 응모 / T: 성공 정확히 100, SOLD_OUT 900, Redis remaining=0, SCARD=100, **seq 100개 모두 다름**, 최종 DB 100건 | NFR-01 |
| T4-29 | **시나리오 (동시성)** | `EntryConcurrencyTest` | G: 정원 100, 사용자 1명 / W: 같은 사용자 10번 동시 응모 / T: 성공 1, DUPLICATE 9, remaining=99, 최종 DB 1건 | NFR-02 |
| T4-30 | 시나리오 (보상) | `EntryCompensationScenarioTest` | G: 발행 실패 Publisher 로 교체 / W: HTTP 응모 / T: 503 `ENTRY_TEMPORARILY_FAILED`, remaining·applicants 원상복구, 같은 사용자 재응모(정상 Publisher) → 202 | NFR-03 |
| T4-31 | **시나리오 (유령 이벤트, 보상이 먼저)** | `GhostEventScenarioTest` | 4.4 참고. T: DB 에 A 1건, B 1건(seq=2), Redis SCARD=2 == DB 2. **v1.5 기대값 변경**: A 는 **seq=3**(재응모 번호) — 유령 이벤트(seq 1)는 requests 의 eventId 불일치로 claim 0 | NFR-03 |
| T4-32 | 시나리오 (재수신) | `DropCreatedRedeliveryTest` | G: `drop.created` 처리 후 응모 30건 / W: 같은 `drop.created` 재발행 / T: remaining=70, drop_snapshots 1행 | REQ-DROP-02 |
| T4-33 | 시나리오 (잘못된 메시지, v1.2) | `DropCreatedRedeliveryTest` | G: capacity < winner 인 `drop.created` / W: 발행 후 정상 `drop.created`(다른 dropId) 발행 / T: 정상 건은 처리됨(drop_snapshots·meta 생성), 잘못된 건은 drop_snapshots·Redis meta 없음, `drop.created.DLT` 수신 0 (재시도·DLT 없이 스킵) | REQ-DROP-02 |
| T4-34 | presentation 통합 (DLT, v1.2) | `EntryCreatedListenerTest` | G: (drop 1, user 1, seq 1) DB 저장, user 2 를 Redis 응모자로 등록 (v1.5: requests 에 메시지와 같은 eventId) / W: `EntryCreated(drop 1, user 2, seq 1)` 발행 (seq 충돌 → `DataIntegrityViolationException`) / T: 재시도 후 `entry.created.DLT` 에서 같은 key 의 레코드 수신, DB 는 1건 그대로 | 6.1 ③ |
| T4-35 | application 단위 (v1.2) | `EntryErrorCodeTest` | `from(ApplyOutcome)` 4가지 매핑, `from(OK)` → `IllegalArgumentException`, 각 코드의 `code()`=`ENTRY_*`·`status()` 가 entry-apply.md 3장 표와 일치 | 설계 |
| T4-36 | infra 통합 (Redis, v1.5) | `RedisEntryGateTest` | G: 초기화, 토큰 있음 / W: tryApply(eventId=e1) / T: OK, `HGET requests userId` = e1. 거절 4종(CLOSED·NO_TOKEN·DUPLICATE·SOLD_OUT)은 requests 변화 없음 (DUPLICATE 는 기존 값 e1 유지) | E-40 |
| T4-37 | infra 통합 (Redis, v1.5) | `RedisEntryGateTest` | **claim Lua**. G: 응모(e1) / W: claimForSave(e1) / T: true, persisted 에 userId, 다시 호출해도 true (멱등). G: 응모자 없음 / T: false, persisted 변화 없음. G: 응모(e1) → 보상(e1) → 재응모(e2) / W: claim(e1) / T: false. claim(e2) → true | E-40 |
| T4-38 | infra 통합 (Redis, v1.5) | `RedisEntryGateTest` | **보상 3결과**. ① 응모(e1) → compensate(e1) = COMPENSATED: remaining +1, applicants·requests 에서 제거, seq 유지. ② 응모(e1) → compensate(e2) = NOT_OURS: remaining·applicants·requests 변화 없음. ③ 판정 없이 compensate(e1) = NOT_OURS. ④ 응모(e1) → claim(e1) → compensate(e1) = ALREADY_PERSISTED: remaining·applicants·requests·persisted 변화 없음 | E-40 |
| T4-39 | infra 통합 (Redis, v1.5) | `RedisEntryGateTest` | **타임아웃 → 포트 예외**. G: 응모 가능 상태, 별도 연결로 `CLIENT PAUSE 1500 ALL` (명령 타임아웃 1초보다 길게) / W: tryApply(e1) / T: `GateUnavailableException`(원인 `DataAccessException` 계열). pause 종료 후 Lua 는 실행돼 있음(applicants·requests=e1) → compensate(e1) = COMPENSATED, remaining 원상복구. 타이밍에 민감하면 pause 시간만 조정한다 | E-41 |
| T4-40 | application 단위 (v1.5) | `EntryApplyServiceTest` | **발행 실패 + 보상 결과별 응답**. G: Gate OK(seq=7), Publisher `failNext()` / ① `nextCompensation(ALREADY_PERSISTED)` → 예외 없이 결과 seq=7 (202). ② COMPENSATED / NOT_OURS → `TEMPORARILY_FAILED`. 모든 경우 보상 호출 1회, eventId 는 tryApply·payload 와 같음 | E-40 |
| T4-41 | application 단위 (v1.5) | `EntryApplyServiceTest` | **Gate 장애 → 보상 + 503**. ① `timeoutAfterApplyNext()` / W: apply / T: `TEMPORARILY_FAILED`, 발행 0, 보상 1회(tryApply 와 같은 eventId) → Fake 상태 원상복구(COMPENSATED). ② `timeoutBeforeApplyNext()` / T: `TEMPORARILY_FAILED`, 보상 1회 → NOT_OURS, 상태 변화 없음. ③ ① + `failCompensateNext()` / T: 여전히 `TEMPORARILY_FAILED` (보상 예외가 원래 예외를 가리지 않음) | E-41 |
| T4-42 | **시나리오 (유령 이벤트, 소비가 먼저, v1.5)** | `GhostEventScenarioTest` | 4.4 순서 B 참고. G: 정원 2, A·B·C 토큰 / W: A 응모(Publisher 가 실제 send → 저장 컨슈머가 A 를 DB 에 저장할 때까지 대기 → 실패를 던짐), B 응모, C 응모 / T: A **202**, B 202, C 409 `ENTRY_SOLD_OUT`. DB 2건(A, B) == Redis SCARD 2, remaining `"0"`, persisted ⊇ {A, B} — **정원 초과 없음** (v1.4 코드에서는 A 가 503 + 보상으로 자리가 풀려 C 가 성공, DB 3건) | E-40 |

### 4.3 동시성 테스트 스케치 (T4-28, T4-29)
application 서비스를 직접 호출한다. HTTP 를 거치면 톰캣 스레드 풀이 동시성을 제한해서 "동시" 요청이 줄어든다. HTTP 경로는 8단계 부하 테스트에서 본다.

```java
@SpringBootTest
@Import({PostgresContainerConfig.class, RedisContainerConfig.class, KafkaContainerConfig.class,
         EntryConcurrencyTest.TopicConfig.class})                // 0단계 testFixtures. TopicConfig: entry.created 등 NewTopic
class EntryConcurrencyTest {

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
                    } catch (ApplicationException e) {
                        failures.computeIfAbsent((EntryErrorCode) e.errorCode(), k -> new AtomicInteger())
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

### 4.4 유령 이벤트 시나리오 재현 (T4-31, T4-42)
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

**순서 A — 보상이 먼저 (T4-31, 설계 5.2 재응모 시퀀스와 같게)**
1. 드로우 초기화(정원 100), A·B 토큰 발급
2. **저장 리스너 일시 정지**: `registry.getListenerContainer("entry-saver").pause()`. 이렇게 해야 A 가 재응모하기 전에 유령 이벤트가 처리되는 걸 막고, 설계 그림의 순서를 그대로 재현할 수 있다.
3. `loseAckOnceFor(A)` → A 응모 → **503** (Kafka 에는 (A, seq 1) 기록, Redis 는 보상됨)
4. B 응모 → 202, seq 2
5. A 재응모 → 202, seq 3
6. 리스너 재개: `resume()`
7. Awaitility 로 확인
   - DB 2건: A 1건(**seq 3**, v1.5 — 유령 이벤트 (A, seq 1, e1) 은 `requests[A]` = e3 ≠ e1 이라 claim 0), B 1건(seq 2)
   - (v1.4 까지는 유령 이벤트가 `isApplicant` 를 통과해 A 가 seq 1 로 저장되고 seq 3 은 user 충돌로 무시됐다)
   - Redis `SCARD applicants` = 2 == DB 건수 → 5단계 추첨이 대기 없이 진행 가능

**변형 케이스 (같은 테스트 클래스에 추가)**
- 2번(일시 정지)을 빼면 유령 이벤트가 A 재응모 전에 처리될 수 있다. 이때는 A 가 응모자에 없어 claim 0 이고, A 는 seq 3 으로 저장된다. **어느 순서든 A·B 각 1건, 합계 2건, A 는 seq 3** 이어야 한다.
- A 가 재응모하지 않는 경우: 유령 이벤트는 무시되고 DB 는 B 1건, Redis SCARD 1 → 일치.

**순서 B — 소비가 보상보다 먼저 (T4-42, v1.5, E-40 재현)**
ack 유실형 유령 이벤트에서 더 흔한 순서다. 리스너를 멈추지 않고, **실제로 보낸 뒤 저장 컨슈머의 처리 완료를 기다린 다음** 실패를 던지는 Publisher 로 재현한다. 보상은 그 뒤에 서비스가 실행한다.
```java
/** 테스트 전용: 지정한 사용자의 첫 발행은 실제로 보내고, 컨슈머가 DB 에 저장할 때까지 기다린 뒤 실패를 던진다. */
class ConsumedThenFailPublisher implements EntryEventPublisher {

    private final EntryEventPublisher delegate;               // 실제 KafkaEntryEventPublisher
    private final EntryJpaRepository entryJpaRepository;
    private final Set<Long> consumedThenFailOnce = ConcurrentHashMap.newKeySet();

    void consumeThenFailOnceFor(long userId) { consumedThenFailOnce.add(userId); }

    @Override
    public void publishCreated(EntryCreatedPayload payload) {
        delegate.publishCreated(payload);                     // 브로커에 기록 → 저장 컨슈머가 claim + INSERT
        if (consumedThenFailOnce.remove(payload.userId())) {
            await().atMost(Duration.ofSeconds(10)).until(() ->
                    entryJpaRepository.findByDropIdAndUserId(payload.dropId(), payload.userId()).isPresent());
            throw new EventPublishException("ack 유실 시뮬레이션 (컨슈머가 먼저 처리)", null);
        }
    }
}
```
1. 드로우 초기화(**정원 2**), A·B·C 토큰 발급. 리스너는 **실행 중** 그대로
2. `consumeThenFailOnceFor(A)` → A 응모 → 컨슈머가 claim(1) + INSERT → Publisher 가 실패를 던짐 → 서비스 보상 = `ALREADY_PERSISTED` → **202**
3. B 응모 → 202 (remaining 0)
4. C 응모 → 409 `ENTRY_SOLD_OUT`
5. Awaitility 로 확인: DB 2건(A, B), Redis `SCARD applicants` = 2, `GET remaining` = `"0"`, `SMEMBERS persisted` ⊇ {A, B}
- v1.4 코드라면 2번에서 보상이 A 의 자리를 풀어(503) C 가 성공하고 DB 는 3건(정원 + 1)이 된다 — 이 테스트가 E-40 회귀 테스트다.
- 기다리는 조건을 "DB 에 행이 있음"으로 둬 claim 이 보상보다 먼저임을 보장한다 (claim 은 INSERT 전에 실행).

---

## 5. 커밋 계획

> v1.2: 3단계와 같이 TDD 사이클(장원영 Red → 고윤정 Green) 단위로, 테스트와 구현을 한 커밋에 넣는다. 4단계부터는 **사이클마다 신민아 검증** (CLAUDE.md). 6.2 설계 수정은 모두 반영 완료 상태라 설계 문서 선행 커밋은 없다.

| # | 커밋 단위 | 커밋 메시지 | 테스트 ID |
|---|-----------|-------------|-----------|
| 1 | domain | `feat(entry): 응모·드로우 스냅샷 도메인 모델과 저장소 포트 추가`<br>- Entry, EntryStatus, ApplyOutcome, DropSnapshot(null·불변식 검증), SnapshotStatus, DomainException<br>- EntryRepository, DropSnapshotRepository | T4-01, 02 |
| 2 | application 응모 | `feat(entry): 응모 유스케이스와 Redis·Kafka 포트 추가`<br>- EntryApplyCommand/Result, EntryGate·GateResult, EntryEventPublisher·EntryCreatedPayload·EventPublishException<br>- EntryErrorCode(ErrorCode 구현, from(ApplyOutcome)), EntryApplyService(판정 → 발행 → 실패 시 보상), ClockConfig | T4-03~06, 35 |
| 3 | application 저장·수신·조회 | `feat(entry): 응모 저장·드로우 스냅샷 등록·내 응모 조회 유스케이스 추가`<br>- EntrySaveService(저장 전 isApplicant 확인), DropSnapshotRegisterService(잘못된 메시지 스킵), EntryQueryService(단건만 Redis 보완) | T4-07~10 |
| 4 | infrastructure DB | `feat(entry): 응모·드로우 스냅샷 테이블과 JPA 저장소 추가`<br>- V1__create_entry_tables.sql<br>- JPA 엔티티·Mapper·RepositoryImpl (INSERT ... ON CONFLICT DO NOTHING)<br>- build.gradle: data-jpa-test | T4-19~22 (+ T0-03 `ApplicationContextTest` 통과 유지) |
| 5 | infrastructure Redis | `feat(entry): Redis Lua 기반 응모 게이트 추가`<br>- entry-initialize/apply/compensate.lua, RedisScriptConfig, EntryRedisKeys, RedisEntryGate<br>- build.gradle: data-redis-test, application.yml: redis timeout | T4-11~18 |
| 6 | infrastructure Kafka | `feat(entry): entry.created 동기 발행 어댑터 추가`<br>- KafkaEntryEventPublisher (get 3.5초, 실패 → EventPublishException), application.yml: linger.ms·await-timeout | T4-23, 24 |
| 7 | presentation API | `feat(entry): 응모·내 응모 조회 API 추가`<br>- EntryController(POST 202, GET 2종), Response | T4-25 |
| 8 | presentation 리스너 | `feat(entry): drop.created·entry.created 리스너와 DLT 에러 핸들러 추가`<br>- DropCreatedListener, EntryCreatedListener (idIsGroup=false), KafkaConsumerConfig(3회 재시도 → DLT), application.yml: listener | T4-26, 32, 33, 34 |
| 9 | 시나리오 | `test(entry): 응모 동시성·보상·유령 이벤트 시나리오 테스트 추가` | T4-27~31 |
| 10a | E-40·E-41 application + 포트 (v1.5) | `fix(entry): 요청 ID 기반 보상 결과 분기와 Redis 장애 시 보상 추가`<br>- EntryGate: tryApply(eventId)·compensate → CompensationResult·claimForSave, CompensationResult, GateUnavailableException<br>- EntryApplyService: 판정 전 eventId 생성, ALREADY_PERSISTED → 202, GateUnavailable → 같은 eventId 로 보상 후 503<br>- EntrySaveCommand.eventId, EntrySaveService: isApplicant → claimForSave, EntryCreatedListener: eventId 전달<br>- FakeEntryGate 갱신 (RedisEntryGate 는 10b 전까지 컴파일만 맞춤 — 10a·10b 를 한 PR 안에서 연속 커밋) | T4-03, 05, 07, 08, 40, 41 |
| 10b | E-40·E-41 Lua + Redis 어댑터 (v1.5) | `fix(entry): 응모 저장 확정 Lua 와 요청 ID 대조 보상 Lua 추가`<br>- entry-apply.lua: KEYS requests·ARGV eventId, HSET<br>- entry-claim.lua 신규, entry-compensate.lua 3결과<br>- EntryRedisKeys.requests/persisted, RedisScriptConfig.entryClaimScript, RedisEntryGate: DataAccessException → GateUnavailableException<br>- GhostEventScenarioTest 순서 A 기대값 A seq=3 (v1.6: 커밋마다 전체 통과를 유지하려고 10c 에서 당김) | T4-16, 17, 26, 31, 34, 36~39 |
| 10c | E-40 시나리오 (v1.5) | `test(entry): 유령 이벤트가 보상보다 먼저 소비되는 시나리오 추가`<br>- GhostEventScenarioTest: 순서 B(ConsumedThenFailPublisher) | T4-42 (+ T4-27~31 전체 재실행) |
| 10d | 검증 반영 | `fix(entry): ...` (신민아·한지민 지적 중 명백한 버그가 그 밖에 있을 때만) | — |
| 11 | 문서 | `docs: 4단계 완료 표시` (플랜 진행 현황, 체크리스트, 백로그 처리 결과) | — |

- v1.5: 10a~c 의 설계 문서 변경(D2 v1.3, entry-apply v1.6, sequence-diagram v1.3, architecture v1.5, class-diagram v2.3, 이 문서 v1.5)은 10a 앞에 `docs: 유령 이벤트·Redis 타임아웃 대응 설계 반영 (E-40, E-41)` 커밋으로 먼저 넣는다. 각 사이클은 장원영 Red → 고윤정 Green → 신민아 검증 순서 그대로.

커밋 본문 예시 (#5)
```
feat(entry): Redis Lua 기반 응모 게이트 추가

- entry-apply.lua: 마감·토큰·중복·잔여 검사 후 DECR + SADD + INCR seq 원자 실행
- entry-compensate.lua: SREM 성공 시에만 INCR remaining (중복 보상 안전, seq 유지)
- entry-initialize.lua: meta 가 없을 때만 초기화 (drop.created 재수신 대비)
- RedisEntryGate: EntryGate 포트 구현. remaining 은 정수 문자열 (queue admit.lua 가 읽음)

Co-Authored-By: ...
```

---

## 6. 결정 필요 · 리스크

### 6.1 이 단계에서 확정하는 것
| # | 항목 | 결정 |
|---|------|------|
| ① | 컨슈머 저장 전 Redis 응모자 확인 | **확정 (2026-11-16 사용자 승인)**. `EntrySaveService` 에서 `isApplicant` 확인 후 없으면 저장하지 않음. → v1.5 에서 claim Lua 로 대체 (⑥) |
| ② | Kafka 동기 발행 정책 | **확정 (2026-11-16)**. 애플리케이션 재시도 없음, producer 내부 재시도 + `request.timeout.ms=1000`, `delivery.timeout.ms=3000`, `get` 대기 3.5초. 쿠폰(6단계)도 같은 정책 |
| ③ | 저장 컨슈머 실패 처리 | **확정 (2026-11-16)**. 3회 재시도 후 `{topic}.DLT`. 단, 도메인 불변식을 어긴 `drop.created` 는 재시도해도 결과가 같으므로 application 이 로그 후 스킵한다 (v1.2, queue 와 같은 방식) |
| ④ | E-34 입장 인원과 잔여 정원 | **확정 (사용자 결정 2026-11-16)**. queue 입장은 잔여 정원과 무관하게 **주기당 N명 유지** (`admit.lua` 변경 없음). 소진 직전 초과 입장자는 이 단계 Lua 가 `SOLD_OUT` 으로 거절한다(정합성 영향 없음). 8단계 부하 테스트에서 소진 직전 `SOLD_OUT` 응답 수를 관찰한다 |
| ⑤ | E-39 Redis Cluster 대비 키 설계 | **확정 (사용자 결정 2026-11-16)**. **단일 Redis 유지, 해시 태그 미적용**. 응모 Lua 가 queue 토큰 키를 KEYS 로 선언해 읽는 현재 형태를 유지한다. Cluster 전환 시 재설계 범위는 백로그 E-39 (해시 태그 또는 토큰 Hash + `HEXPIRE`) |
| ⑥ | E-40 유령 이벤트가 보상보다 먼저 소비되는 경우 | **확정 (사용자 결정 2026-11-17)**. 요청 ID = eventId 를 판정 전에 만들고 `requests` Hash 에 기록. 저장 컨슈머는 `entry-claim.lua`(applicants·requests 확인 후 `SADD persisted`)로 저장 확정, 보상 Lua 는 COMPENSATED / ALREADY_PERSISTED / NOT_OURS. ALREADY_PERSISTED 면 202. ① 의 `isApplicant` 확인을 대체한다. 포기한 대안: 5단계 집합 대조(쿠폰에 적용 불가, 조회 비용), 보상 생략(유실 시 D6 정지). 키 TTL 은 E-42 와 함께 5단계 |
| ⑦ | E-41 판정 Lua 응답 타임아웃 | **확정 (사용자 결정 2026-11-17)**. `RedisEntryGate` 가 `DataAccessException` 을 `GateUnavailableException` 으로 감싸고, `EntryApplyService` 는 같은 eventId 로 보상을 시도한 뒤 503 `ENTRY_TEMPORARILY_FAILED`. 보상도 실패하면 로그만 (6.3 리스크). 5단계 대조 여부는 5단계에서 판단 |

### 6.2 설계 수정 필요 (구현 전에 설계 문서부터 고침)
| # | 대상 문서 | 현재 | 변경 |
|---|-----------|------|------|
| ① | [entry-apply.md](../../design/05-features/entry-apply.md) 3장, [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D2, [implementation-plan.md](../implementation-plan.md) 4단계 | `get(3s)` + 재시도 2회 | 애플리케이션 재시도 없음. producer 내부 재시도(멱등) + `request.timeout.ms=1000`, `delivery.timeout.ms=3000`, `get` 대기 3.5초 → 설계 반영 완료 (2026-11-16) |
| ② | [architecture.md](../../design/02-architecture/architecture.md) 4장, [infrastructure.md](../../design/03-infrastructure/infrastructure.md) | DLT 없음 | `drop.created.DLT`, `entry.created.DLT`(+ 6단계 `coupon.issued.DLT`) 토픽 추가, 재시도 3회 후 DLT → 설계 반영 완료 (2026-11-16) |
| ③ | [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.1 | `EntryRepository.save(Entry) Entry`, `existsByDropIdAndUserId` | `saveIfAbsent(Entry) boolean`, `findByDropIdAndUserId`, `findAllByUserId` → 설계 반영 완료 (2026-11-16) |
| ④ | [entry-apply.md](../../design/05-features/entry-apply.md) 3장 API 설명 | "조회 API 는 DB 에 없으면 Redis 로 보완" | **단건 조회(`/drops/{dropId}/me`)만** 보완. 목록(`/me`)은 DB 만 → 설계 반영 완료 (2026-11-16) |
| ⑤ | [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 3장 | `DropCreated` 에 `productId` 없음 | [architecture.md](../../design/02-architecture/architecture.md) 4장 페이로드에는 있음 → 둘 중 하나로 통일 (entry 는 쓰지 않으므로 어느 쪽이든 무방, ranking·queue 확인 필요) → 설계 반영 완료 (2026-11-16) |
| ⑥ | [entry-apply.md](../../design/05-features/entry-apply.md) 3장, [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.5 | 에러 코드 5개 | `ENTRY_NOT_FOUND`(404), `COMMON_MISSING_USER_HEADER`(400) 추가 → 설계 반영 완료 (2026-11-16, 코드 명명은 0단계 entry-apply v1.4). `InvalidDropSnapshotException` 은 ⑦ 로 대체 |
| ⑦ | [entry-apply.md](../../design/05-features/entry-apply.md) 3장, [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.5, [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.1·1.3 | 잘못된 `drop.created` → `InvalidDropSnapshotException` → 리스너 재시도 후 `drop.created.DLT`. 실패 응답은 `EntryApplyException` + 별도 `@RestControllerAdvice` | 잘못된 `drop.created` 는 application 이 `DomainException` 을 잡아 로그·스킵 (queue 와 같음). 실패 응답은 `ApplicationException(EntryErrorCode)` + `GlobalExceptionHandler` → 설계 반영 완료 (2026-11-16, v1.2) |
| ⑧ | [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D2·D3, [entry-apply.md](../../design/05-features/entry-apply.md) 3장·5.2·6장, [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 2.2 ③·5장, [architecture.md](../../design/02-architecture/architecture.md) 5장, [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.1 | 저장 전 `SISMEMBER` 확인, 보상 = `SREM`→`INCR`, Redis 장애 시 500 | 요청 ID(`requests`)·저장 확정(`persisted`)·claim Lua·보상 3결과·ALREADY_PERSISTED → 202·`GateUnavailableException` → 보상 후 503 (6.1 ⑥⑦) → 설계 반영 완료 (2026-11-17, D2 v1.3 / entry-apply v1.6 / sequence v1.3 / architecture v1.5 / class v2.3) |

### 6.3 리스크
| 리스크 | 영향 | 대응 |
|--------|------|------|
| Kafka 지연이 응답 시간에 그대로 들어감 | 응모 p99 가 최대 3.5초까지 늘 수 있음 | 8단계 부하 테스트로 측정. `linger.ms`, 파티션 수 조정 |
| `TimeoutException` 후 뒤늦은 기록 (유령 이벤트) | 보상된 응모가 Kafka 에 남음. 소비가 보상보다 먼저면 v1.4 까지는 DB 응모 수 = 정원 + 1 (E-40) | v1.5: claim Lua + 3결과 보상 + requests 대조 + seq 카운터. 어느 순서든 "DB 저장 ⇔ Redis 응모자 유지" (T4-31, T4-42) |
| 보상 Lua 실패 (발행 실패 경로) | 정원 한 자리가 영구히 비어 있음 (언더 응모) | 로그 알림. 정원 초과는 아님 (D2) |
| 스크립트 캐시가 없을 때 판정 타임아웃 (재시작 직후·`SCRIPT FLUSH` 뒤 첫 호출, v1.6) | EVALSHA 가 타임아웃되면 NOSCRIPT → EVAL 재시도가 일어나지 않아 Lua 가 실행되지 않음 | 안전: 보상이 NOT_OURS 로 끝나고 503, 재시도하면 정상 판정. T4-39 는 늦게 도착한 Lua 경로를 보려고 pause 전에 스크립트를 캐시에 올린다 |
| 판정 Lua 응답 타임아웃 + 보상도 실패 (E-41) | Redis 에만 응모(Kafka 메시지 없음) → 언더 응모 + D6 건수 불일치로 추첨 대기. 사용자는 재시도 시 409 DUPLICATE, 단건 조회는 저장 대기(APPLIED) | log.error 알림 후 운영 대응. 5단계에서 Redis ∖ DB 대조 여부 판단 (백로그 E-41 개선안 2) |
| claim 후 DB 저장 실패 (DLT) | Redis 응모자 있음, DB 없음 → D6 대기 | v1.5 이후 남는 유일한 Redis·DB 불일치. DLT 재처리로 복구 (claim 은 재처리에도 1) |
| 새 키 `requests`·`persisted` 메모리 | 정원 크기만큼 Hash(UUID 36B 값)·Set 이 드로우마다 추가로 남음 | 지금 규모에서는 문제없음. TTL 은 E-42 와 함께 5단계에서 결정 |
| 6단계 쿠폰이 같은 발행·보상 패턴 | 같은 E-40·E-41 문제가 초과 발급으로 나타남 | phase-6 문서 리스크에 메모. 설계 확정은 6단계 |
| Redis 재시작 시 데이터 유실 | 오버 응모 가능 | 0단계의 AOF everysec. 최대 1초 유실 가능성은 남음 |
| Redis Cluster 미지원 | 키가 해시태그 없이 여러 슬롯에 흩어져 Lua 다중 키 실행 불가 | 단일 Redis 전제 유지 (6.1 ⑤ 사용자 결정). Cluster 전환 시 백로그 E-39 대로 queue·entry 키를 함께 재설계 |
| 소진 직전 초과 입장 (E-34) | 잔여 정원보다 많은 토큰 보유자가 응모해 `SOLD_OUT` 응답이 몰림 | 정합성은 응모 Lua 가 지킴. 8단계에서 `SOLD_OUT` 응답 수 관찰 (6.1 ④) |
| 서버 시계 차이 | 인스턴스마다 `now` 가 달라 마감 경계에서 판정이 조금씩 다를 수 있음 | 판정은 Redis 한 곳에서 하므로 결과가 갈리진 않음. 서버 NTP 동기화 전제 |
| `drop.created` 전파 지연 | 오픈 직후 `CLOSED` 응답 | 드로우 등록 시 `openAt` 최소 1분 이후 검증 (2단계 확정 규칙) |
| queue 토큰 키 형식 의존 | queue 가 키 형식을 바꾸면 응모가 전부 `NO_TOKEN` | `EntryRedisKeys.queueToken` 주석(= `QueueKeys.token`) + 단계 종료 시 로컬 compose 에서 실제 queue 와 함께 진입 → 입장 → 응모 확인 |
| `entry:{dropId}:remaining` 형식 의존 (역방향) | entry 가 정수가 아닌 값을 쓰면 queue `admit.lua`·`RedisRemainingCapacityReader` 가 깨짐 | `SET`/`DECR`/`INCR` 만 사용, T4-11 에서 정수 문자열 확인 (3.0 키 계약 표) |

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
| (v1.5) 유령 이벤트가 보상보다 먼저 소비 → 202, DB == Redis, 정원 초과 없음 | T4-37, T4-38, T4-40, T4-42 |
| (v1.5) 판정 Lua 타임아웃 → 보상 후 503 | T4-39, T4-41 |
| 같은 이벤트 재전달 → DB 1건 유지 | T4-08, T4-19, T4-26 |
| `drop.created` 재수신 → 잔여 정원 덮어쓰기 없음 | T4-18, T4-22, T4-32 |
| (v1.2) 잘못된 `drop.created` → 스킵, 저장 실패 → `entry.created.DLT` | T4-09, T4-33, T4-34 |

### 공통 완료 조건
- [ ] `./gradlew :entry-service:build` 통과 (T4-27 ArchUnit 포함)
- [ ] API 3개가 [entry-apply.md](../../design/05-features/entry-apply.md) API 표와 일치 (경로, 202, 에러 코드)
- [x] 6.2 "설계 수정 필요" ①~⑦ 설계 문서 반영 + 변경 이력 추가 (2026-11-16)
- [ ] 단계 종료 시 로컬 compose 에서 드로우 등록 → 대기열 진입 → 입장 → 응모 202 → 내 응모 조회 확인 (queue 키 계약 실검증)
- [x] 6.1 결정 ① 을 플랜 4장 "미결정 사항" 표와 설계 README "확인 필요" 에서 확정으로 갱신 (2026-11-16)
- [ ] 플랜 진행 현황에서 4단계 ✅
