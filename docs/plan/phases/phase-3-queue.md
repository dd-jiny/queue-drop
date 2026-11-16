# 3단계 — 대기열 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 설계 반영 완료 표시 |
| v1.2 | 2026-11-16 | 0단계 에러 계약에 정렬: `QueueErrorCode implements ErrorCode`(코드 `QUEUE_*`), 별도 `QueueException`·`QueueExceptionHandler` 제거(`ApplicationException` + 0단계 `GlobalExceptionHandler`), 헤더 누락은 `COMMON_MISSING_USER_HEADER`(400). 도메인 불변식 위반은 `DomainException`, 잘못된 `drop.created` 는 로그·스킵. Kafka 설정은 0단계 공통 블록 유지(ErrorHandlingDeserializer). 스케줄러 on/off 속성 추가. 결정 필요 2건 기본안 확정(CLOSED 상태 없음, 키 정리 `closeAt + 1일` 구현). 커밋 계획을 테스트 포함 기능 커밋 단위로 재편 |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 오픈된 드로우의 대기열에 진입해 순번을 받고, 스케줄러가 1초마다 앞에서 N명에게 입장 토큰(TTL 5분)을 발급한다 |
| 요구사항 | REQ-QUEUE-01 ~ REQ-QUEUE-06 (06 은 토큰 **발급** 까지. 토큰 **검증** 은 4단계 응모 Lua) |
| 대상 서비스 | queue-service (DB 없음, Redis 전용) |
| 브랜치 | `feat/phase-3-queue` (`dev` 에서 생성) |
| 선행 조건 | 0단계(common `DropCreated`·`Topics`, ArchUnit, Testcontainers Redis/Kafka, 공통 에러 응답), 1단계(`X-User-Id` 주입), 2단계(`drop.created` 발행) |
| 제약 | entry-service 가 아직 없으므로 잔여 정원 키 `entry:{dropId}:remaining` 은 **테스트에서 직접 세팅**한다 (3.5) |

**참고 설계 문서**
- [queue-enter.md](../../design/05-features/queue-enter.md) — 상태, 진입, 입장 스케줄러, API, 리스크
- [architecture.md](../../design/02-architecture/architecture.md) 5장 — Redis 키 소유권
- [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D3, D4
- [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.3
- [layered-architecture.md](../../design/02-architecture/layered-architecture.md)
- [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 2.2 ②

---

## 2. 산출물 파일 목록

```
queue-service/
├── build.gradle                                        # (수정) 테스트 의존성
└── src
    ├── main
    │   ├── java/com/queuedrop/queue
    │   │   ├── QueueServiceApplication.java
    │   │   ├── domain
    │   │   │   ├── model
    │   │   │   │   ├── QueueMeta.java
    │   │   │   │   ├── QueueState.java
    │   │   │   │   └── EnqueueResult.java
    │   │   │   ├── exception
    │   │   │   │   └── DomainException.java                 # v1.2
    │   │   │   └── repository
    │   │   │       └── WaitingQueueRepository.java
    │   │   ├── application
    │   │   │   ├── service
    │   │   │   │   ├── QueueService.java
    │   │   │   │   ├── AdmissionService.java
    │   │   │   │   ├── QueueMetaRegisterService.java
    │   │   │   │   └── PollIntervalPolicy.java
    │   │   │   ├── dto
    │   │   │   │   ├── QueueEnterCommand.java
    │   │   │   │   ├── QueueStatusQuery.java
    │   │   │   │   ├── QueueStatusResult.java
    │   │   │   │   └── QueueMetaRegisterCommand.java
    │   │   │   ├── port
    │   │   │   │   ├── RemainingCapacityReader.java
    │   │   │   │   └── SchedulerGate.java
    │   │   │   ├── config
    │   │   │   │   └── AdmissionProperties.java
    │   │   │   └── exception
    │   │   │       └── QueueErrorCode.java                  # common.error.ErrorCode 구현 (v1.2)
    │   │   ├── infrastructure
    │   │   │   ├── redis
    │   │   │   │   ├── QueueKeys.java
    │   │   │   │   ├── RedisWaitingQueueRepository.java
    │   │   │   │   ├── RedisRemainingCapacityReader.java
    │   │   │   │   └── RedisSchedulerGate.java
    │   │   │   └── config
    │   │   │       ├── ClockConfig.java
    │   │   │       ├── RedisScriptConfig.java
    │   │   │       └── SchedulingConfig.java
    │   │   └── presentation
    │   │       ├── api
    │   │       │   ├── QueueController.java
    │   │       │   ├── GlobalExceptionHandler.java          # 0단계에서 생성됨 (수정 없음)
    │   │       │   └── response/QueueStatusResponse.java
    │   │       ├── listener
    │   │       │   └── DropCreatedListener.java
    │   │       └── scheduler
    │   │           └── AdmissionScheduler.java
    │   └── resources
    │       ├── application.yml                          # (수정) 입장 설정, consumer 역직렬화
    │       └── scripts
    │           ├── enqueue.lua
    │           └── admit.lua
    └── test/java/com/queuedrop/queue
        ├── ArchitectureTest.java
        ├── domain/model/QueueMetaTest.java
        ├── application/service
        │   ├── QueueServiceTest.java
        │   ├── AdmissionServiceTest.java
        │   ├── PollIntervalPolicyTest.java
        │   ├── QueueMetaRegisterServiceTest.java        # v1.2 (T3-26)
        │   └── fake
        │       ├── FakeWaitingQueueRepository.java
        │       ├── FakeRemainingCapacityReader.java
        │       └── FakeSchedulerGate.java
        ├── infrastructure/redis
        │   ├── RedisWaitingQueueRepositoryTest.java
        │   ├── RedisSchedulerGateTest.java
        │   └── support/EntryRemainingFixture.java
        ├── presentation
        │   ├── api/QueueControllerTest.java
        │   └── listener/DropCreatedListenerIntegrationTest.java
        └── AdmissionConcurrencyIntegrationTest.java
```

---

## 3. 구현 상세

### 3.0 설정

**build.gradle** (queue-service)
```groovy
dependencies {
    implementation project(':common')
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    implementation 'org.springframework.boot:spring-boot-starter-kafka'

    testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
    testImplementation 'org.springframework.boot:spring-boot-starter-data-redis-test'   // @DataRedisTest 슬라이스
    testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'
    // testFixtures(project(':common')) 은 루트 build.gradle 이 4계층 서비스에 일괄 추가 (0단계)
}
```

**application.yml** — v1.2: Kafka·Redis 블록은 0단계에서 만든 공통 설정(`ErrorHandlingDeserializer` + Jackson delegate, `group-id: ${spring.application.name}`)을 그대로 쓰고 아래 `queue.admission` 만 추가한다.
```yaml
queue:
  admission:
    interval-ms: 1000         # 입장 주기
    batch-size: 100           # 주기당 입장 인원 N (1~1000)
    token-ttl-seconds: 300    # 입장 토큰 TTL
    scheduler-enabled: true   # false 면 AdmissionScheduler 빈을 만들지 않는다 (테스트에서 직접 admit() 호출)
```

### 3.0.1 Redis 키 (queue 소유 + 읽기 전용 1개)

| 키 | 타입 | 쓰기 | 값 | 비고 |
|----|------|------|-----|------|
| `queue:{dropId}:meta` | Hash | queue | `openAt`, `closeAt` (epoch ms) | `drop.created` 수신 시 HSET, `PEXPIREAT closeAt + 1일` (v1.2) |
| `queue:{dropId}:seq` | String | queue | 진입 순번 카운터 | `enqueue.lua` 에서 INCR, `PEXPIREAT closeAt + 1일` (v1.2) |
| `queue:{dropId}:waiting` | ZSet | queue | member=userId, score=seq | `enqueue.lua` 에서 `PEXPIREAT closeAt + 1일` (v1.2) |
| `queue:{dropId}:token:{userId}` | String (TTL) | queue | 발급 시각 epoch ms | 존재 = 유효 토큰, entry 가 읽음 |
| `queue:active-drops` | ZSet | queue | member=dropId, score=closeAt | **신규 (설계 수정 필요)** 스케줄러 대상 조회 |
| `queue:admission:gate` | String (PX) | queue | 인스턴스 ID | **신규 (설계 수정 필요)** 주기당 1회 실행 게이트 |
| `entry:{dropId}:remaining` | String | **entry** | 잔여 정원 | queue 는 **읽기만** |

> `queue:active-drops` 가 필요한 이유: 설계는 "오픈 중인 드로우 목록은 queue meta 에서 조회"라고만 했는데, `queue:*:meta` 를 `KEYS`/`SCAN` 하는 것은 운영 Redis 에서 금지 수준의 비용이다. 마감 시각을 score 로 둔 인덱스 ZSet 으로 대체한다. **(구현 결정)**

---

### 3.1 domain

#### `QueueMeta` — 드로우별 대기열 운영 시각
```java
package com.queuedrop.queue.domain.model;

/** 시각은 epoch ms 로 저장되므로 생성 시 밀리초로 내림한다 (DropCreated 는 µs 정밀도, 차이 < 1ms) */
public record QueueMeta(Long dropId, Instant openAt, Instant closeAt) {
    public QueueMeta {
        if (dropId == null || openAt == null || closeAt == null) throw new DomainException("필수 값 누락");
        openAt = openAt.truncatedTo(ChronoUnit.MILLIS);
        closeAt = closeAt.truncatedTo(ChronoUnit.MILLIS);
        if (!openAt.isBefore(closeAt)) throw new DomainException("openAt < closeAt 이어야 합니다");
    }
    /** openAt <= now < closeAt */
    public boolean isOpen(Instant now) { return !now.isBefore(openAt) && now.isBefore(closeAt); }
    public boolean isBeforeOpen(Instant now) { return now.isBefore(openAt); }
    public boolean isClosed(Instant now) { return !now.isBefore(closeAt); }
}
```

#### `QueueState` — 사용자 상태
```java
public enum QueueState { WAITING, ADMITTED, SOLD_OUT, NOT_IN_QUEUE }
```

#### `EnqueueResult` — 진입 결과 (도메인 값)
```java
/** admitted=true 면 이미 토큰 보유. 아니면 rank(0-based, 내 앞 인원 수). */
public record EnqueueResult(boolean admitted, long rank) {
    public static EnqueueResult alreadyAdmitted() { return new EnqueueResult(true, -1); }
    public static EnqueueResult waiting(long rank) { return new EnqueueResult(false, rank); }
}
```

#### `WaitingQueueRepository` — 대기열 저장소 포트 (도메인 개념이므로 domain 에 둔다, class-diagram 2.3)
```java
package com.queuedrop.queue.domain.repository;

public interface WaitingQueueRepository {
    /** 멱등. 같은 드로우 재수신 시 덮어써도 값이 동일 */
    void saveMeta(QueueMeta meta);
    Optional<QueueMeta> findMeta(Long dropId);

    /** 원자적: 토큰 보유 → alreadyAdmitted / 대기 중 → 현재 rank / 신규 → seq 발급 후 rank */
    EnqueueResult enqueue(Long dropId, Long userId);

    boolean hasToken(Long dropId, Long userId);
    /** 대기 중이 아니면 empty */
    OptionalLong rank(Long dropId, Long userId);

    /** 앞에서 최대 n 명을 꺼내 토큰 발급. 잔여 정원 키가 없거나 0 이하이면 아무도 입장시키지 않는다. 입장한 userId 목록 반환 */
    List<Long> admit(Long dropId, int n, Duration tokenTtl, Instant now);

    /** closeAt > now 인 드로우 ID (openAt 판단은 호출 측) */
    List<Long> findActiveDropIds(Instant now);
    void removeExpiredDrops(Instant now);
}
```
> `admit` 이 잔여 정원 키(entry 소유)까지 Lua 안에서 확인하는 것은 원자성 때문이다 (3.3 `admit.lua`). 도메인 포트 시그니처에는 정원 키 이름이 드러나지 않는다.

---

### 3.2 application

#### Command / Query / Result
```java
public record QueueEnterCommand(Long dropId, Long userId) {}
public record QueueStatusQuery(Long dropId, Long userId) {}
public record QueueMetaRegisterCommand(Long dropId, Instant openAt, Instant closeAt) {}

/** state 는 String (presentation 이 domain enum 을 보지 않도록). 값은 QueueState.name() 에서 만든다 */
public record QueueStatusResult(String state, Long aheadCount, long nextPollAfterMs) {
    public static QueueStatusResult waiting(long ahead, long poll) { return new QueueStatusResult(QueueState.WAITING.name(), ahead, poll); }
    public static QueueStatusResult admitted() { return new QueueStatusResult(QueueState.ADMITTED.name(), null, 0); }
    public static QueueStatusResult soldOut() { return new QueueStatusResult(QueueState.SOLD_OUT.name(), null, 0); }
    public static QueueStatusResult notInQueue() { return new QueueStatusResult(QueueState.NOT_IN_QUEUE.name(), null, 0); }
}
```
- `nextPollAfterMs` 를 **응답에 포함한다** (queue-enter.md 6장 폴링 부하 완화). `0` = 폴링 중단. **(구현 결정)**

#### 포트
```java
package com.queuedrop.queue.application.port;

/** entry-service 소유 키 읽기 (D3). 키가 없으면 empty = "아직 초기화 안 됨" */
public interface RemainingCapacityReader {
    OptionalLong read(Long dropId);
}

/** 다중 인스턴스에서 입장 처리가 주기당 1회만 실행되도록 하는 게이트 */
public interface SchedulerGate {
    boolean tryPass(String name, Duration window);
}
```

**"키 없음" 처리 정책 (구현 결정)**
| 상황 | 진입(enter) | 상태 조회(status) | 입장 스케줄러 |
|------|-------------|-------------------|---------------|
| 키 없음 (entry 가 아직 `drop.created` 미처리) | **허용** (WAITING) | WAITING/NOT_IN_QUEUE 정상 판정 | 해당 드로우 **건너뜀** |
| 잔여 ≤ 0 | **409 SOLD_OUT** | `SOLD_OUT` | 건너뜀 |
| 잔여 > 0 | 정상 | 정상 | 입장 처리 |

> 이유: 두 서비스의 `drop.created` 소비 순서는 보장되지 않는다. 키가 없을 때 진입을 막으면 오픈 직후 정상 사용자가 거절될 수 있고, 반대로 입장까지 시키면 토큰을 받았는데 응모가 실패한다. "줄은 세우되 입장은 보류"가 양쪽 위험을 모두 피한다.

#### 에러 코드 (v1.2: 0단계 에러 계약)
```java
package com.queuedrop.queue.application.exception;

public enum QueueErrorCode implements ErrorCode {
    DROP_NOT_FOUND("QUEUE_DROP_NOT_FOUND", 404, "대기열 정보가 없는 드로우입니다."),   // meta 없음 (drop.created 미수신 포함)
    NOT_OPEN("QUEUE_NOT_OPEN", 409, "아직 오픈 전입니다."),
    CLOSED("QUEUE_CLOSED", 409, "응모가 마감되었습니다."),
    SOLD_OUT("QUEUE_SOLD_OUT", 409, "응모 정원이 모두 찼습니다.");
    // code, status, message
}
```
- 예외는 `throw new ApplicationException(QueueErrorCode.X)`. 별도 `QueueException`·`QueueExceptionHandler` 는 두지 않는다 — 0단계 `presentation/api/GlobalExceptionHandler` 가 `ErrorCode.status()` 로 매핑한다.
- `X-User-Id` 누락 → `COMMON_MISSING_USER_HEADER`(400), 숫자가 아님 → `COMMON_INVALID_INPUT`(400).

#### `PollIntervalPolicy` — 다음 폴링 간격 (구현 결정)
```java
@Component
public class PollIntervalPolicy {
    public long nextPollAfterMs(long aheadCount) {
        if (aheadCount < 100) return 1_000;
        if (aheadCount < 1_000) return 3_000;
        return 5_000;
    }
}
```

#### `QueueService` — 진입·상태 조회
```java
@Service
@RequiredArgsConstructor
public class QueueService {
    private final WaitingQueueRepository queueRepository;
    private final RemainingCapacityReader remainingReader;
    private final PollIntervalPolicy pollPolicy;
    private final Clock clock;

    public QueueStatusResult enter(QueueEnterCommand cmd) {
        QueueMeta meta = queueRepository.findMeta(cmd.dropId())
                .orElseThrow(() -> new ApplicationException(QueueErrorCode.DROP_NOT_FOUND));
        Instant now = clock.instant();
        if (meta.isBeforeOpen(now)) throw new ApplicationException(QueueErrorCode.NOT_OPEN);
        if (meta.isClosed(now)) throw new ApplicationException(QueueErrorCode.CLOSED);
        if (isSoldOut(cmd.dropId())) throw new ApplicationException(QueueErrorCode.SOLD_OUT);

        EnqueueResult r = queueRepository.enqueue(cmd.dropId(), cmd.userId());
        return r.admitted() ? QueueStatusResult.admitted()
                            : QueueStatusResult.waiting(r.rank(), pollPolicy.nextPollAfterMs(r.rank()));
    }

    public QueueStatusResult status(QueueStatusQuery q) {
        queueRepository.findMeta(q.dropId())
                .orElseThrow(() -> new ApplicationException(QueueErrorCode.DROP_NOT_FOUND));
        if (isSoldOut(q.dropId())) return QueueStatusResult.soldOut();          // 1순위
        if (queueRepository.hasToken(q.dropId(), q.userId())) return QueueStatusResult.admitted();
        OptionalLong rank = queueRepository.rank(q.dropId(), q.userId());
        return rank.isPresent()
                ? QueueStatusResult.waiting(rank.getAsLong(), pollPolicy.nextPollAfterMs(rank.getAsLong()))
                : QueueStatusResult.notInQueue();
    }

    private boolean isSoldOut(Long dropId) {
        OptionalLong remaining = remainingReader.read(dropId);
        return remaining.isPresent() && remaining.getAsLong() <= 0;
    }
}
```
- 상태 판정 우선순위: **SOLD_OUT > ADMITTED > WAITING > NOT_IN_QUEUE**. 정원이 찼으면 토큰이 있어도 응모가 실패하므로 SOLD_OUT 을 먼저 알린다. **(구현 결정)**
- 상태 조회에서 마감(`closeAt` 지남)은 따로 표시하지 않는다 — 마감 후엔 대기·토큰이 의미 없으므로 클라이언트는 드로우 상세(product)로 판단. 필요하면 `CLOSED` 상태 추가 (6장).
- `@Transactional` 없음 (DB 없음).

#### `AdmissionService` — 주기적 입장 처리
```java
@Slf4j
@Service
@RequiredArgsConstructor
public class AdmissionService {
    static final String GATE_NAME = "admission";

    private final WaitingQueueRepository queueRepository;
    private final SchedulerGate schedulerGate;
    private final AdmissionProperties props;
    private final Clock clock;

    /** @return 이번 주기에 입장한 총 인원 (게이트 미통과 시 0) */
    public int admit() {
        if (!schedulerGate.tryPass(GATE_NAME, Duration.ofMillis(props.intervalMs()))) return 0;
        Instant now = clock.instant();
        queueRepository.removeExpiredDrops(now);
        int total = 0;
        for (Long dropId : queueRepository.findActiveDropIds(now)) {
            QueueMeta meta = queueRepository.findMeta(dropId).orElse(null);
            if (meta == null || !meta.isOpen(now)) continue;
            List<Long> admitted = queueRepository.admit(dropId, props.batchSize(),
                    Duration.ofSeconds(props.tokenTtlSeconds()), now);
            total += admitted.size();
            if (!admitted.isEmpty()) log.debug("dropId={} admitted={}", dropId, admitted.size());
        }
        return total;
    }
}
```
- 잔여 정원 확인은 `admit.lua` 안에서 원자적으로 하므로 여기서 `RemainingCapacityReader` 를 다시 부르지 않는다.
- 배치 크기 N 은 **드로우마다** 적용 (드로우 3개 오픈 시 주기당 최대 3N). **(구현 결정)**

#### `AdmissionProperties`
```java
/** application 계층 설정 값 객체. 바인딩(@ConfigurationProperties)은 infrastructure/config 에서 수행 */
public record AdmissionProperties(long intervalMs, int batchSize, long tokenTtlSeconds) {}
```
> `@ConfigurationProperties` 는 Spring Boot 어노테이션이므로 application 에 두지 않는다. infrastructure 의 설정 클래스가 `queue.admission.*` 을 읽어 이 record 를 Bean 으로 등록. **(구현 결정)**

#### `QueueMetaRegisterService` — `drop.created` 처리
```java
@Service
@RequiredArgsConstructor
public class QueueMetaRegisterService {
    private final WaitingQueueRepository queueRepository;

    /** 불변식을 어긴 메시지(필수 값 누락, openAt ≥ closeAt)는 재시도해도 같으므로 warn 로그 후 스킵한다 (v1.2) */
    public void register(QueueMetaRegisterCommand cmd) {
        QueueMeta meta;
        try {
            meta = new QueueMeta(cmd.dropId(), cmd.openAt(), cmd.closeAt());
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", cmd.dropId(), e.getMessage());
            return;
        }
        queueRepository.saveMeta(meta);
    }
}
```
- queue 는 저장 컨슈머가 아니므로 DLT 를 쓰지 않는다 (phase-0 DLT 정책: 저장 컨슈머 entry·coupon 만). Redis 장애로 `saveMeta` 가 실패하면 예외를 올려 Spring Kafka 기본 `DefaultErrorHandler` 재시도에 맡긴다. 역직렬화 실패는 0단계 `ErrorHandlingDeserializer` 가 로그·스킵한다.

---

### 3.3 infrastructure

#### `QueueKeys` — 키 이름 단일 출처
```java
public final class QueueKeys {
    public static final String ACTIVE_DROPS = "queue:active-drops";
    public static String meta(Long dropId)    { return "queue:" + dropId + ":meta"; }
    public static String seq(Long dropId)     { return "queue:" + dropId + ":seq"; }
    public static String waiting(Long dropId) { return "queue:" + dropId + ":waiting"; }
    public static String tokenPrefix(Long dropId) { return "queue:" + dropId + ":token:"; }
    public static String token(Long dropId, Long userId) { return tokenPrefix(dropId) + userId; }
    /** 스케줄러 게이트 queue:{name}:gate (v1.2) */
    public static String gate(String name) { return "queue:" + name + ":gate"; }
    /** entry-service 소유 — 읽기 전용 */
    public static String entryRemaining(Long dropId) { return "entry:" + dropId + ":remaining"; }
}
```

#### `enqueue.lua` — 진입 (원자)
```lua
-- KEYS[1] = queue:{dropId}:waiting
-- KEYS[2] = queue:{dropId}:seq
-- KEYS[3] = queue:{dropId}:token:{userId}
-- KEYS[4] = queue:{dropId}:meta
-- ARGV[1] = userId
-- ARGV[2] = 키 보존 기간 ms (closeAt 이후, 기본 86400000 = 1일)
-- return: -1 = 이미 토큰 보유(ADMITTED), 0 이상 = 0-based rank (내 앞 인원 수)

if redis.call('EXISTS', KEYS[3]) == 1 then
    return -1
end

local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
if rank then
    return rank
end

local seq = redis.call('INCR', KEYS[2])
redis.call('ZADD', KEYS[1], 'NX', seq, ARGV[1])

-- v1.2 키 정리: waiting·seq 는 closeAt + 보존 기간에 만료 (ZPOPMIN 으로 비면 키가 사라지므로 신규 등록마다 다시 건다)
local closeAt = redis.call('HGET', KEYS[4], 'closeAt')
if closeAt then
    local expireAt = tonumber(closeAt) + tonumber(ARGV[2])
    redis.call('PEXPIREAT', KEYS[1], expireAt)
    redis.call('PEXPIREAT', KEYS[2], expireAt)
end
return redis.call('ZRANK', KEYS[1], ARGV[1])
```
> 설계(queue-enter.md 5.2)는 `INCR → ZADD NX` 2개 명령을 허용했지만, **토큰 보유 확인 → 대기 확인 → 신규 등록을 하나의 Lua 로 묶는다.** 효과: (1) 동시 중복 요청에서 seq 낭비 없음, (2) "hasToken 확인 직후 입장되어 다시 줄을 서는" 경합 제거, (3) 왕복 1회. **(구현 결정, 설계 수정 필요)**

#### `admit.lua` — 입장 (원자: 잔여 정원 확인 + ZPOPMIN + SET token)
```lua
-- KEYS[1] = queue:{dropId}:waiting
-- KEYS[2] = entry:{dropId}:remaining   (entry 소유, 읽기만)
-- ARGV[1] = n (배치 크기)
-- ARGV[2] = token TTL (초)
-- ARGV[3] = token 키 prefix  "queue:{dropId}:token:"
-- ARGV[4] = 발급 시각 epoch ms (토큰 값)
-- return: 입장한 userId 배열

local remaining = redis.call('GET', KEYS[2])
if not remaining or tonumber(remaining) <= 0 then
    return {}
end

local popped = redis.call('ZPOPMIN', KEYS[1], tonumber(ARGV[1]))
local admitted = {}
for i = 1, #popped, 2 do
    local userId = popped[i]
    redis.call('SET', ARGV[3] .. userId, ARGV[4], 'EX', tonumber(ARGV[2]))
    admitted[#admitted + 1] = userId
end
return admitted
```
- `ZPOPMIN` 과 `SET token` 을 묶어 "줄에서 빠졌는데 토큰 없음" 상태를 없앤다 (queue-enter.md 5.2 권장 사항).
- 반복문은 **최대 N 회로 유한**하다 (N=100 기본). N 을 크게 늘리면 Redis 블로킹 시간이 늘어나므로 1,000 이하로 제한. **(구현 결정)**
- 토큰 키를 스크립트 안에서 조합하므로 Redis **Cluster 에서는 사용할 수 없다** (키 선언 규칙 위반). 현재 단일 Redis 전제 (6장 리스크).

#### `RedisScriptConfig`
```java
@Configuration
public class RedisScriptConfig {
    @Bean public RedisScript<Long> enqueueScript() {
        return RedisScript.of(new ClassPathResource("scripts/enqueue.lua"), Long.class);
    }
    @Bean public RedisScript<List> admitScript() {
        return RedisScript.of(new ClassPathResource("scripts/admit.lua"), List.class);
    }
}
```
> Spring Data Redis 는 `EVALSHA` 를 먼저 시도하고 없으면 `EVAL` 로 재시도한다.

#### `RedisWaitingQueueRepository` — domain 포트 구현
```java
@Repository
@RequiredArgsConstructor
public class RedisWaitingQueueRepository implements WaitingQueueRepository {
    private final StringRedisTemplate redis;
    private final RedisScript<Long> enqueueScript;
    private final RedisScript<List> admitScript;

    /** 보존 기간: closeAt 이후 queue 키(meta·waiting·seq)를 남겨 두는 시간 (v1.2, 기본안 1일) */
    static final Duration RETENTION_AFTER_CLOSE = Duration.ofDays(1);

    @Override
    public void saveMeta(QueueMeta meta) {
        String key = QueueKeys.meta(meta.dropId());
        redis.opsForHash().putAll(key, Map.of(
                "openAt", String.valueOf(meta.openAt().toEpochMilli()),
                "closeAt", String.valueOf(meta.closeAt().toEpochMilli())));
        redis.expireAt(key, meta.closeAt().plus(RETENTION_AFTER_CLOSE));   // PEXPIREAT. 이미 지났으면 즉시 삭제
        redis.opsForZSet().add(QueueKeys.ACTIVE_DROPS, String.valueOf(meta.dropId()),
                meta.closeAt().toEpochMilli());
    }

    @Override
    public Optional<QueueMeta> findMeta(Long dropId) {
        Map<Object, Object> h = redis.opsForHash().entries(QueueKeys.meta(dropId));
        if (h.isEmpty()) return Optional.empty();
        return Optional.of(new QueueMeta(dropId,
                Instant.ofEpochMilli(Long.parseLong((String) h.get("openAt"))),
                Instant.ofEpochMilli(Long.parseLong((String) h.get("closeAt")))));
    }

    @Override
    public EnqueueResult enqueue(Long dropId, Long userId) {
        Long r = redis.execute(enqueueScript,
                List.of(QueueKeys.waiting(dropId), QueueKeys.seq(dropId), QueueKeys.token(dropId, userId),
                        QueueKeys.meta(dropId)),
                String.valueOf(userId), String.valueOf(RETENTION_AFTER_CLOSE.toMillis()));
        return r == -1 ? EnqueueResult.alreadyAdmitted() : EnqueueResult.waiting(r);
    }

    @Override
    public boolean hasToken(Long dropId, Long userId) {
        return Boolean.TRUE.equals(redis.hasKey(QueueKeys.token(dropId, userId)));
    }

    @Override
    public OptionalLong rank(Long dropId, Long userId) {
        Long r = redis.opsForZSet().rank(QueueKeys.waiting(dropId), String.valueOf(userId));
        return r == null ? OptionalLong.empty() : OptionalLong.of(r);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Long> admit(Long dropId, int n, Duration tokenTtl, Instant now) {
        List<String> ids = redis.execute(admitScript,
                List.of(QueueKeys.waiting(dropId), QueueKeys.entryRemaining(dropId)),
                String.valueOf(n), String.valueOf(tokenTtl.toSeconds()),
                QueueKeys.tokenPrefix(dropId), String.valueOf(now.toEpochMilli()));
        return ids == null ? List.of() : ids.stream().map(Long::valueOf).toList();
    }

    @Override
    public List<Long> findActiveDropIds(Instant now) {
        Set<String> ids = redis.opsForZSet().rangeByScore(QueueKeys.ACTIVE_DROPS,
                now.toEpochMilli() + 1, Double.POSITIVE_INFINITY);   // closeAt > now
        return ids == null ? List.of() : ids.stream().map(Long::valueOf).toList();
    }

    @Override
    public void removeExpiredDrops(Instant now) {
        redis.opsForZSet().removeRangeByScore(QueueKeys.ACTIVE_DROPS, Double.NEGATIVE_INFINITY, now.toEpochMilli());
    }
}
```

#### `RedisRemainingCapacityReader` — application 포트 구현 (entry 소유 키 읽기)
```java
@Component
@RequiredArgsConstructor
public class RedisRemainingCapacityReader implements RemainingCapacityReader {
    private final StringRedisTemplate redis;

    @Override
    public OptionalLong read(Long dropId) {
        String v = redis.opsForValue().get(QueueKeys.entryRemaining(dropId));
        return v == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(v));
    }
}
```

#### `RedisSchedulerGate` — 주기당 1회 실행 게이트 (분산 락)
```java
@Component
@RequiredArgsConstructor
public class RedisSchedulerGate implements SchedulerGate {
    private final StringRedisTemplate redis;
    private final String instanceId = UUID.randomUUID().toString();

    /** SET queue:{name}:gate {instanceId} NX PX {window}. 해제하지 않고 만료에 맡긴다. */
    @Override
    public boolean tryPass(String name, Duration window) {
        return Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent(QueueKeys.gate(name), instanceId, window));
    }
}
```
> **락을 해제(DEL)하지 않는 이유 (구현 결정)**: 일반 분산 락처럼 작업 후 해제하면, A 가 50ms 만에 끝내고 해제한 직후 B 의 주기가 와서 다시 실행 → 1초에 2N 명이 입장한다. 게이트를 **주기 길이(1초) 동안 유지**하면 인스턴스 수와 무관하게 "전체에서 1초당 1회"가 보장된다. 즉 락이 아니라 **전역 레이트 게이트**다.
> 결과적으로 키 이름은 `queue:admission:gate` (3.0.1 표).

#### 설정 클래스
```java
@Configuration
public class ClockConfig { @Bean Clock clock() { return Clock.systemUTC(); } }

@Configuration
@EnableScheduling
public class SchedulingConfig {
    @Bean
    AdmissionProperties admissionProperties(
            @Value("${queue.admission.interval-ms}") long intervalMs,
            @Value("${queue.admission.batch-size}") int batchSize,
            @Value("${queue.admission.token-ttl-seconds}") long ttl) {
        if (batchSize < 1 || batchSize > 1000) throw new IllegalStateException("batch-size 는 1~1000");
        if (intervalMs < 1 || ttl < 1) throw new IllegalStateException("interval-ms·token-ttl-seconds 는 1 이상");
        return new AdmissionProperties(intervalMs, batchSize, ttl);
    }
}
```

---

### 3.4 presentation

#### `QueueStatusResponse`
```java
public record QueueStatusResponse(String state, Long aheadCount, long nextPollAfterMs) {
    public static QueueStatusResponse from(QueueStatusResult r) {
        return new QueueStatusResponse(r.state(), r.aheadCount(), r.nextPollAfterMs());
    }
}
```
응답 예:
```json
{ "state": "WAITING", "aheadCount": 1523, "nextPollAfterMs": 5000 }
{ "state": "ADMITTED", "aheadCount": null, "nextPollAfterMs": 0 }
```

#### `QueueController`
```java
@RestController
@RequestMapping("/api/queue/drops")
@RequiredArgsConstructor
public class QueueController {
    private final QueueService queueService;

    @PostMapping("/{dropId:\\d+}")
    public QueueStatusResponse enter(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return QueueStatusResponse.from(queueService.enter(new QueueEnterCommand(dropId, userId)));
    }

    @GetMapping("/{dropId:\\d+}")
    public QueueStatusResponse status(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return QueueStatusResponse.from(queueService.status(new QueueStatusQuery(dropId, userId)));
    }
}
```
| Method | Path | 성공 | 실패 |
|--------|------|------|------|
| POST | `/api/queue/drops/{dropId}` | 200 (WAITING / ADMITTED) | 404 `QUEUE_DROP_NOT_FOUND`, 409 `QUEUE_NOT_OPEN` / `QUEUE_CLOSED` / `QUEUE_SOLD_OUT`, 400 `COMMON_MISSING_USER_HEADER` |
| GET | `/api/queue/drops/{dropId}` | 200 (WAITING / ADMITTED / SOLD_OUT / NOT_IN_QUEUE) | 404 `QUEUE_DROP_NOT_FOUND`, 400 `COMMON_MISSING_USER_HEADER` |

> 진입 응답은 새로 줄을 서든 기존 순번을 돌려주든 **200** 으로 통일 (멱등 진입). **(구현 결정)**

#### `AdmissionScheduler` — 시간이 보내는 요청을 받는 입력 어댑터
```java
@Component
@ConditionalOnProperty(name = "queue.admission.scheduler-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class AdmissionScheduler {
    private final AdmissionService admissionService;

    @Scheduled(fixedRateString = "${queue.admission.interval-ms}")
    public void run() {
        admissionService.admit();
    }
}
```
> 테스트에서는 `queue.admission.scheduler-enabled=false` 로 스케줄러 빈을 만들지 않고 `AdmissionService.admit()` 을 직접 호출한다 (v1.2).

#### `DropCreatedListener`
```java
@Component
@RequiredArgsConstructor
public class DropCreatedListener {
    private final QueueMetaRegisterService registerService;

    @KafkaListener(topics = Topics.DROP_CREATED)   // group-id 는 0단계 공통 설정 ${spring.application.name}
    public void on(DropCreated message) {
        registerService.register(new QueueMetaRegisterCommand(
                message.dropId(), message.openAt(), message.closeAt()));
    }
}
```
- 멱등: 같은 메시지 재수신 시 HSET/ZADD 가 같은 값으로 덮어쓴다. 대기열(`waiting`, `seq`)은 건드리지 않는다.

#### 예외 매핑 (v1.2)
0단계 `GlobalExceptionHandler` 를 그대로 쓴다. `ApplicationException(QueueErrorCode.*)` → `ErrorCode.status()`, `X-User-Id` 누락 → 400 `COMMON_MISSING_USER_HEADER`, 숫자가 아닌 `X-User-Id` → 400 `COMMON_INVALID_INPUT`, 숫자가 아닌 dropId → 404 `COMMON_NOT_FOUND`(경로 정규식 불일치).

---

### 3.5 테스트에서 잔여 정원 키 세팅

entry-service 가 없으므로 테스트 픽스처가 entry 의 역할을 대신한다.

```java
// test/.../infrastructure/redis/support/EntryRemainingFixture.java
public class EntryRemainingFixture {
    private final StringRedisTemplate redis;
    public EntryRemainingFixture(StringRedisTemplate redis) { this.redis = redis; }

    /** entry-service 가 drop.created 처리 시 하는 SET NX 를 흉내낸다 */
    public void initialize(Long dropId, long capacity) {
        redis.opsForValue().set("entry:" + dropId + ":remaining", String.valueOf(capacity));
    }
    public void soldOut(Long dropId) { initialize(dropId, 0); }
    public void clear(Long dropId) { redis.delete("entry:" + dropId + ":remaining"); }
}
```
- 키 이름은 `architecture.md` 5장과 일치해야 하며, 4단계에서 entry 의 실제 초기화 코드와 같은 키인지 4단계 통합 테스트에서 다시 확인한다.
- 로컬 수동 확인: `docker exec -it queuedrop-redis redis-cli SET entry:1:remaining 100`

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ |
|----|-----------|---------------|-------------------------------|----------|
| T3-01 | domain 단위 | `QueueMetaTest` | Given openAt=10:00, closeAt=12:00 / When isOpen(09:59:59.999)=false, (10:00)=true, (11:59:59.999)=true, (12:00)=false | REQ-QUEUE-02 |
| T3-02 | domain 단위 | `QueueMetaTest` | openAt ≥ closeAt 로 생성 → `DomainException`, 필수 값 null → `DomainException`, µs 시각 → ms 내림 (v1.2) | REQ-QUEUE-01 |
| T3-03 | application 단위 | `QueueServiceTest` | Given meta 없음 / When enter·status / Then `ApplicationException(DROP_NOT_FOUND)` | REQ-QUEUE-01 |
| T3-04 | application 단위 | `QueueServiceTest` | Given 오픈 전 / When enter / Then NOT_OPEN. 마감 후 → CLOSED | REQ-QUEUE-02 |
| T3-05 | application 단위 | `QueueServiceTest` | Given 잔여 0 / When enter / Then SOLD_OUT 예외. status → SOLD_OUT (토큰 보유자도) | REQ-QUEUE-03 |
| T3-06 | application 단위 | `QueueServiceTest` | Given 잔여 키 없음 / When enter / Then WAITING (진입 허용) | REQ-QUEUE-02 |
| T3-07 | application 단위 | `QueueServiceTest` | Given 신규 / When enter 2회 / Then 두 번 모두 같은 aheadCount (멱등) | REQ-QUEUE-02 |
| T3-08 | application 단위 | `QueueServiceTest` | status 분기: 토큰 보유 → ADMITTED, 대기 → WAITING(aheadCount), 둘 다 아님 → NOT_IN_QUEUE | REQ-QUEUE-03 |
| T3-09 | application 단위 | `PollIntervalPolicyTest` | aheadCount 0·99 → 1000, 100·999 → 3000, 1000 → 5000 | REQ-QUEUE-03 |
| T3-10 | application 단위 | `AdmissionServiceTest` | Given FakeSchedulerGate 불통과 / When admit / Then 0, 저장소 호출 없음 | REQ-QUEUE-04 |
| T3-11 | application 단위 | `AdmissionServiceTest` | Given 활성 드로우 2개 중 1개 오픈 전 / When admit / Then 오픈된 드로우만 admit 호출 | REQ-QUEUE-04 |
| T3-12 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | Given 사용자 A,B,C 순서 enqueue / Then rank 0,1,2 | REQ-QUEUE-02 |
| T3-13 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | **같은 사용자 10개 스레드 동시 enqueue → ZSet 멤버 1개, seq 값 1** | REQ-QUEUE-02 |
| T3-14 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | Given 토큰 보유 사용자 / When enqueue / Then alreadyAdmitted, ZSet 에 추가되지 않음 | REQ-QUEUE-02 |
| T3-15 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | **Given 250명 대기, 잔여 1000 / When admit(n=100) 3회 / Then 100·100·50 입장, 전원 토큰 보유, 입장 순서 = 진입 순서, 대기열 비어 있음** | REQ-QUEUE-04 |
| T3-16 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | Given TTL 1초로 admit / When 1.5초 후 / Then hasToken=false, rank 없음 → NOT_IN_QUEUE | REQ-QUEUE-05 |
| T3-17 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | **Given 잔여 0 (soldOut) / When admit / Then 입장 0, 대기열 유지** | REQ-QUEUE-04 |
| T3-18 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | Given 잔여 키 없음 / When admit / Then 입장 0 | REQ-QUEUE-04 |
| T3-19 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | findActiveDropIds: closeAt 지난 드로우 제외, removeExpiredDrops 후 ZSet 에서 제거 | REQ-QUEUE-04 |
| T3-20 | infra 통합 (Redis) | `RedisSchedulerGateTest` | Given window 1초 / When 같은 이름으로 2회 tryPass / Then true, false. 1.1초 후 다시 true | REQ-QUEUE-04 |
| T3-21 | 통합 (Redis, 다중 인스턴스) | `AdmissionConcurrencyIntegrationTest` | **Given 대기 500명, n=100, AdmissionService 2개(서로 다른 Gate 인스턴스 ID) / When 동시에 admit / Then 이번 주기 입장 총 100명** | REQ-QUEUE-04 |
| T3-22 | 통합 (Redis+Kafka) | `DropCreatedListenerIntegrationTest` | Given `drop.created` 발행 / Then meta Hash·active-drops 등록. 같은 메시지 재발행 → 값 동일, 기존 waiting 유지 | REQ-QUEUE-01 |
| T3-23 | presentation 슬라이스 | `QueueControllerTest` | `ApplicationException(QueueErrorCode.*)` 각 코드 → 404/409 + `code`·고정 `message`, `X-User-Id` 누락 → 400 `COMMON_MISSING_USER_HEADER`, 응답 필드(state, aheadCount, nextPollAfterMs) | REQ-QUEUE-02, 03 |
| T3-24 | 아키텍처 | `ArchitectureTest` | R1~R7 통과 (presentation 이 `QueueState` 미참조, application 에 Spring Data Redis 미참조) | — |
| T3-25 | infra 통합 (Redis) | `RedisWaitingQueueRepositoryTest` | (v1.2 키 정리) saveMeta 후 meta 의 만료 시각 = closeAt + 1일, enqueue 후 waiting·seq 도 같은 만료 시각. 대기열이 비었다가 다시 enqueue 해도 만료가 다시 걸림 | REQ-QUEUE-01 |
| T3-26 | application 단위 | `QueueMetaRegisterServiceTest` | (v1.2) 정상 명령 → saveMeta 1회. openAt ≥ closeAt 또는 null 필드 → 예외 없이 저장 안 함 | REQ-QUEUE-01 |

---

## 5. 커밋 계획

> v1.2: 1·2단계와 같이 TDD 사이클(장원영 Red → 카리나 Green) 단위로, 테스트와 구현을 한 커밋에 넣는다. 신민아 검증은 단계 끝에 한 번 (1~3단계 합의).

| # | 커밋 단위 | 커밋 메시지 | 테스트 ID |
|---|-----------|-------------|-----------|
| 1 | domain | `feat(queue): 대기열 도메인 모델과 저장소 포트 추가`<br>- QueueMeta(isOpen, ms 내림), QueueState, EnqueueResult, DomainException<br>- WaitingQueueRepository | T3-01, 02 |
| 2 | application | `feat(queue): 대기열 진입·상태 조회·입장 유스케이스 추가`<br>- 상태 판정 우선순위, 잔여 정원 키 없음 정책, QueueErrorCode<br>- RemainingCapacityReader·SchedulerGate 포트, 폴링 간격 정책, 잘못된 drop.created 스킵 | T3-03~11, 26 |
| 3 | infrastructure Redis | `feat(queue): Redis 대기열 저장소와 진입·입장 Lua 추가`<br>- enqueue.lua (토큰·대기 확인 + 순번 발급 + 만료 원자 처리)<br>- admit.lua (잔여 정원 확인 + ZPOPMIN + 토큰 발급)<br>- 활성 드로우 인덱스, 설정(`queue.admission`) 바인딩 | T3-12~19, 25 |
| 4 | infrastructure 게이트 | `feat(queue): 입장 스케줄러 전역 게이트 추가` (SET NX PX, 미해제) | T3-20, 21 |
| 5 | presentation | `feat(queue): 대기열 API·입장 스케줄러·drop.created 리스너 추가` | T3-22, 23 |
| 6 | 검증 반영 | `fix(queue): ...` (신민아·한지민 지적 중 명백한 버그가 있을 때만) | — |
| 7 | 문서 | `docs: 3단계 완료 표시` (플랜 진행 현황, 결정 사항) | — |

---

## 6. 결정 필요 · 리스크

### 구현 결정 요약
| 항목 | 결정 |
|------|------|
| 진입 처리 | `enqueue.lua` 하나로 토큰 확인 + 대기 확인 + seq 발급 + ZADD (원자) |
| 활성 드로우 조회 | `queue:active-drops` ZSet (score=closeAt). `KEYS`/`SCAN` 미사용 |
| 스케줄러 중복 방지 | `queue:admission:gate` `SET NX PX {interval}` **미해제** 전역 레이트 게이트 |
| 잔여 정원 키 없음 | 진입 허용, 입장 보류 |
| 잔여 정원 확인 위치 | `admit.lua` 안 (원자) + 진입·상태 조회 시 `RemainingCapacityReader` |
| 상태 우선순위 | SOLD_OUT > ADMITTED > WAITING > NOT_IN_QUEUE |
| 폴링 응답 | `nextPollAfterMs` 포함 (1s / 3s / 5s) |
| 배치 크기 | 드로우별 N 적용, 최대 1,000 |
| 진입 응답 코드 | 신규·재진입 모두 200 |
| 설정 바인딩 | `AdmissionProperties` record 는 application, 바인딩은 infrastructure |
| 에러 계약 (v1.2) | `QueueErrorCode implements ErrorCode` + `ApplicationException`, 0단계 `GlobalExceptionHandler` |
| 잘못된 `drop.created` (v1.2) | 도메인 불변식 위반은 warn 로그 후 스킵 (queue 는 저장 컨슈머가 아니므로 DLT 없음) |
| 시각 정밀도 (v1.2) | queue meta 는 epoch ms. `DropCreated` 의 µs 는 ms 로 내림 (오픈·마감 판정이 product 보다 최대 1ms 이르다) |

### 설계 수정 필요
- [architecture.md](../../design/02-architecture/architecture.md) 5장 Redis 키 표에 `queue:active-drops`(ZSet), `queue:admission:gate`(String PX) 추가. → 설계 반영 완료 (2026-11-16)
- [queue-enter.md](../../design/05-features/queue-enter.md) 3장·5.2: 진입을 `INCR → ZADD NX` 2명령에서 **`enqueue.lua` 원자 처리**로 변경, 입장 Lua 에 잔여 정원 확인 포함, 스케줄러 분산 락을 "미해제 게이트"로 명시, `nextPollAfterMs` 응답 필드 확정, 잔여 정원 키 없음 정책 추가. → 설계 반영 완료 (2026-11-16)
- [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.3: `EnqueueResult`, `QueueMetaRegisterService`, `SchedulerGate` 포트, `PollIntervalPolicy` 추가. `WaitingQueueRepository` 메서드 시그니처(`saveMeta`, `findMeta`, `findActiveDropIds`, `removeExpiredDrops`) 반영. → 설계 반영 완료 (2026-11-16)

### 결정 필요
- [x] 상태 조회에서 마감 후(`closeAt` 경과) 별도 `CLOSED` 상태를 둘지 → **확정(기본안)**: 두지 않는다. 클라이언트는 드로우 상세(product)로 판단 (v1.2)
- [x] 마감된 드로우의 queue 키(`waiting`, `seq`, `meta`) 정리 시점 → **확정(기본안)**: 이 단계에서 구현. `meta` 는 `saveMeta`, `waiting`·`seq` 는 `enqueue.lua` 에서 `PEXPIREAT closeAt + 1일` (v1.2, T3-25). 토큰 키는 자체 TTL, `active-drops` 는 `removeExpiredDrops` 가 정리

### 리스크
- **Redis Cluster 비호환**: `admit.lua` 가 토큰 키를 스크립트 안에서 조합하고, entry 소유 키를 함께 다룬다. 단일 Redis 전제이며, Cluster 전환 시 해시 태그(`{drop:1}`)로 키 재설계 필요.
- **게이트 시간 오차**: 게이트 TTL(1초)과 `fixedRate` 주기가 인스턴스별로 어긋나면 실제 입장 간격이 1~2초 사이로 흔들릴 수 있다. 처리량이 "초당 N명 이하"로만 보장된다는 점을 8단계 리포트에 기록.
- **입장 후 미응모**: 토큰 TTL(5분) 동안 응모하지 않는 사용자만큼 입장 슬롯이 낭비된다. 정원이 남아 있는 한 스케줄러가 계속 입장시키므로 정원은 결국 채워지지만, 오픈 직후 응모 속도는 느려질 수 있다.
- **잔여 정원 키 공유**: entry 가 키 이름·형식을 바꾸면 queue 가 조용히 "키 없음"으로 판단해 입장이 멈춘다. 4단계에서 두 서비스가 같은 키를 쓰는지 통합 테스트로 고정한다.
- **폴링 부하**: 대기자 × (1/폴링 간격) 만큼 GET 요청. `nextPollAfterMs` 를 클라이언트가 지키지 않으면 효과 없음 → 8단계 부하 테스트 시나리오가 이 값을 따르도록 작성.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 테스트 ID |
|----------------|-----------|
| 단위: `QueueMeta.isOpen` 경계값, 상태 판정 분기 | T3-01, T3-02, T3-03 ~ T3-09 |
| 통합: 같은 사용자가 동시에 10번 진입 → 순번 1개 | T3-13 |
| 통합: 대기 250명, 배치 100 → 3번 실행 후 전원 토큰 보유, 순서 유지 | T3-15 |
| 통합: 토큰 TTL 만료 후 `NOT_IN_QUEUE` | T3-16 |
| 통합: 잔여 정원 0 → 입장 중단, 상태 `SOLD_OUT` | T3-05, T3-17 |
| 통합: 스케줄러 2개 동시 실행 → 한 주기에 N명만 입장 (분산 락 동작) | T3-20, T3-21 |
| (추가) 잔여 정원 키 없음 정책 | T3-06, T3-18 |
| (추가) `drop.created` 수신·재수신 멱등 | T3-22 |
| 공통: `./gradlew build` 통과 (ArchUnit 포함) | T3-24 |
| 공통: API 가 설계 문서 API 표와 일치 | T3-23 |
| 공통: 설계 수정 필요 항목 반영 | 6장 "설계 수정 필요" |

- [ ] 위 테스트 전부 통과
- [ ] 로컬: 2단계로 드로우 등록 → `redis-cli SET entry:{id}:remaining 100` → 대기열 진입 → 1초 내 ADMITTED 확인
- [ ] [implementation-plan.md](../implementation-plan.md) 진행 현황 갱신
