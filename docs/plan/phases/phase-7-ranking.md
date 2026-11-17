# 7단계 — 랭킹 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 리스너 실패 처리(로그·스킵) 확정 및 DLT 도입 시 토픽 이름 분리 주의 추가, 설계 반영 완료 표시 |
| v1.2 | 2026-11-17 | 4~6단계 계약에 맞춤 (착수 전 결정 반영): 사용자 결정 2건(인기 드로우 = 진행 중만, 동점 = member 사전순) 확정. `RankingErrorCode implements ErrorCode`(`RANKING_INVALID_SIZE`·`RANKING_INVALID_WEEK` 400, `RANKING_TEMPORARILY_FAILED` 503) + `ApplicationException`, `RankingException`·`RankingExceptionHandler` 삭제(기존 `GlobalExceptionHandler` 사용). `/users/me` 는 `X-User-Id` 필수(누락 400 `COMMON_MISSING_USER_HEADER`), `UNAUTHENTICATED` 삭제. Redis `timeout: 1s`(E-03), Redis 예외 → `RankingStoreUnavailableException`(domain/exception) → 조회 503, 리스너는 전파. Command 는 박싱 타입, 필드 오류는 `DomainException` → WARN 후 스킵(저장소 호출 없음). `KafkaConsumerConfig`(1초 3회 후 로그·스킵, 역직렬화 실패는 재시도 없음). 표시 fallback `user-{id}`·`drop-{id}`. 종료 드로우 lazy ZREM(기본안). `RedisScriptConfig`·`ClockConfig` 분리, rank.lua 쓰기를 끝으로, 스캔 조기 종료. 테스트 T7-25~30 추가, T7-19·20·21·22 수정, Kafka/Redis 테스트 격리 규칙. 커밋 #0 추가 |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | `entry.created` 를 소비해 **인기 드로우 / 주간 유저 / 누적 유저** 랭킹 3종을 집계하고 조회 API 4개를 제공한다 |
| 요구사항 | REQ-RANK-01 (인기 드로우), REQ-RANK-02 (유저 활동 주간·누적), REQ-RANK-03 (비동기 소비) |
| 브랜치 | `feat/phase-7-ranking` |
| 선행 조건 | 0단계(common 이벤트·`Topics`, ArchUnit, Testcontainers), 1단계(`user.registered` 발행), 2단계(`drop.created` 발행), 4단계(`entry.created` 발행) 완료 |
| 참고 설계 | [ranking.md](../../design/05-features/ranking.md), [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.5, [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 4장, [architecture.md](../../design/02-architecture/architecture.md) 4·5장, [layered-architecture.md](../../design/02-architecture/layered-architecture.md) |

**이 단계의 핵심**
- ranking-service 는 **DB 가 없고 다른 서비스를 호출하지 않는다.** 쓰기(Listener → Lua)와 읽기(Controller → ZSet 조회)가 완전히 분리된다.
- 멱등성은 `(dropId, userId)` dedupe Set 으로 확보한다. 요구사항 "1인 1드로우 1응모"와 정확히 같은 키라서 eventId 가 필요 없다.
- 주차는 **처리 시각이 아니라 이벤트의 `appliedAt`** 으로 계산한다.
- 실패는 두 갈래다: **메시지 불변식 위반**(필드 null·잘못된 값)은 WARN 후 스킵(재시도 없음), **Redis 장애**는 리스너에서 1초 간격 3회 재시도 후 로그·스킵, 조회에서는 503 `RANKING_TEMPORARILY_FAILED`.

---

## 2. 산출물 파일 목록

```
ranking-service/
├── build.gradle                                                    (변경 없음)
└── src/
    ├── main/
    │   ├── java/com/queuedrop/ranking/
    │   │   ├── domain/
    │   │   │   ├── model/
    │   │   │   │   ├── WeekKey.java                                [신규]
    │   │   │   │   ├── RankingBoard.java                           [신규]
    │   │   │   │   ├── RankEntry.java                              [신규]
    │   │   │   │   ├── DropWindow.java                             [신규] (구현 결정)
    │   │   │   │   └── EventFields.java                            [신규] (메시지 필드 검증, 구현 결정)
    │   │   │   ├── exception/
    │   │   │   │   ├── DomainException.java                        [신규] (다른 서비스와 같은 형태)
    │   │   │   │   ├── InvalidWeekKeyException.java                [신규] (extends DomainException)
    │   │   │   │   └── RankingStoreUnavailableException.java       [신규] (Redis 장애, DomainException 아님)
    │   │   │   └── repository/
    │   │   │       ├── RankingRepository.java                      [신규]
    │   │   │       ├── DisplayNameRepository.java                  [신규]
    │   │   │       └── DropWindowRepository.java                   [신규] (구현 결정)
    │   │   ├── application/
    │   │   │   ├── service/
    │   │   │   │   ├── RankingAggregateService.java                [신규]
    │   │   │   │   └── RankingQueryService.java                    [신규]
    │   │   │   ├── dto/
    │   │   │   │   ├── RankingRecordCommand.java                   [신규]
    │   │   │   │   ├── DropInfoCommand.java                        [신규]
    │   │   │   │   ├── UserInfoCommand.java                        [신규]
    │   │   │   │   ├── DropRankResult.java                         [신규]
    │   │   │   │   ├── UserRankResult.java                         [신규]
    │   │   │   │   ├── WeeklyRankingResult.java                    [신규]
    │   │   │   │   └── MyRankResult.java                           [신규]
    │   │   │   └── exception/
    │   │   │       └── RankingErrorCode.java                       [신규] (implements ErrorCode)
    │   │   ├── infrastructure/
    │   │   │   ├── redis/
    │   │   │   │   ├── RankingKeys.java                            [신규]
    │   │   │   │   ├── RedisRankingRepository.java                 [신규]
    │   │   │   │   ├── RedisDisplayNameRepository.java             [신규]
    │   │   │   │   └── RedisDropWindowRepository.java              [신규] (구현 결정)
    │   │   │   └── config/
    │   │   │       ├── RankingProperties.java                      [신규]
    │   │   │       ├── RedisScriptConfig.java                      [신규] (Lua 스크립트 Bean, public)
    │   │   │       ├── ClockConfig.java                            [신규] (Clock Bean, 다른 서비스와 같음)
    │   │   │       └── KafkaConsumerConfig.java                    [신규] (에러 핸들러, public)
    │   │   └── presentation/
    │   │       ├── api/
    │   │       │   ├── RankingController.java                      [신규]
    │   │       │   ├── GlobalExceptionHandler.java                 (0단계 작성분 유지 — ApplicationException·헤더 누락 처리)
    │   │       │   └── response/
    │   │       │       ├── DropRankingResponse.java                [신규]
    │   │       │       ├── UserRankingResponse.java                [신규]
    │   │       │       └── MyRankResponse.java                     [신규]
    │   │       └── listener/
    │   │           ├── EntryCreatedListener.java                   [신규]
    │   │           ├── DropCreatedListener.java                    [신규]
    │   │           └── UserRegisteredListener.java                 [신규]
    │   └── resources/
    │       ├── application.yml                                     [수정] (Redis timeout 1s, listener ack-mode, ranking.* 프로퍼티)
    │       └── scripts/rank.lua                                    [신규]
    └── test/java/com/queuedrop/ranking/
        ├── ArchitectureTest.java                                   (0단계 작성분 유지)
        ├── ApplicationContextTest.java                             [수정] (리스너 auto-startup=false — entry·coupon 과 같음)
        ├── support/KafkaTopicReadiness.java                        (0단계 작성분 유지)
        ├── support/KafkaConsumption.java                           [신규] (coupon 과 같은 형태, APP_GROUP = ranking-service)
        ├── domain/model/WeekKeyTest.java                           [신규]
        ├── domain/model/DropWindowTest.java                        [신규]
        ├── application/fake/InMemoryRankingRepository.java         [신규] (호출 기록·장애 주입)
        ├── application/fake/InMemoryDisplayNameRepository.java     [신규]
        ├── application/fake/InMemoryDropWindowRepository.java      [신규]
        ├── application/service/RankingAggregateServiceTest.java    [신규]
        ├── application/service/RankingQueryServiceTest.java        [신규]
        ├── infrastructure/redis/RedisRankingRepositoryTest.java    [신규] (Testcontainers Redis, 리스너 미기동)
        ├── infrastructure/redis/RedisDisplayNameRepositoryTest.java[신규] (Testcontainers Redis, 리스너 미기동)
        ├── presentation/api/RankingControllerTest.java             [신규] (@WebMvcTest)
        ├── presentation/listener/RankingListenerIntegrationTest.java      [신규] (Testcontainers Kafka + Redis)
        └── presentation/listener/RankingListenerRetryIntegrationTest.java [신규] (재시도·역직렬화 실패)
```
> `presentation/api/GlobalExceptionHandler` 는 0단계에서 이미 `ApplicationException` → `ErrorCode.status()`, `MissingRequestHeaderException` → 400 `COMMON_MISSING_USER_HEADER` 를 처리한다. 랭킹 전용 핸들러는 만들지 않는다 ([layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.5).

---

## 3. 구현 상세

### 3.0 Redis 키 (설계 [architecture.md](../../design/02-architecture/architecture.md) 5장 + 추가분)

| 키 | 타입 | TTL | 설명 |
|----|------|-----|------|
| `rank:drop:popular` | ZSet | 없음 | member=dropId, score=응모 수. 종료 드로우는 조회 중 lazy ZREM (3.2) |
| `rank:user:weekly:{yyyy-Www}` | ZSet | **35일** (첫 기록 시 설정) | member=userId, score=주간 응모 수 |
| `rank:user:all` | ZSet | 없음 | member=userId, score=누적 응모 수 |
| `rank:dedupe:{dropId}` | Set | **closeAt + 7일** (첫 기록 시 설정) | 이미 집계한 userId |
| `rank:drop:info` | Hash | 없음 | dropId → title |
| `rank:user:info` | Hash | 없음 | userId → nickname |
| `rank:drop:closeAt` | Hash | 없음 | dropId → closeAt(epoch ms) **(구현 결정 — 설계 문서에 없는 키, 6장 "설계 수정 필요")** |

> `rank:drop:closeAt` 이 필요한 이유: (1) 인기 드로우를 "진행 중 드로우만" 보여주는 필터 (확정, 2026-11-17 사용자 결정), (2) dedupe Set 의 만료 시각 계산.
> 값 하나(epoch ms)만 저장해 Lua 에서 `tonumber()` 로 바로 쓰도록 `rank:drop:info` 와 분리했다.

### 3.1 domain

#### `WeekKey` — ISO 주차 값 객체 (Asia/Seoul)
책임: `Instant` → ISO 주차(`2026-W47`) 변환과 문자열 파싱·검증. `java.time` 만 사용한다.

```java
package com.queuedrop.ranking.domain.model;

import java.time.*;
import java.time.temporal.IsoFields;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record WeekKey(int weekBasedYear, int week) {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final Pattern FORMAT = Pattern.compile("^(\\d{4})-W(\\d{2})$");

    public WeekKey {
        if (week < 1 || week > weeksIn(weekBasedYear)) {
            throw new InvalidWeekKeyException(weekBasedYear + "-W" + week);
        }
    }

    /** 이벤트 발생 시각 기준 주차. 처리 시각(now)을 쓰지 않는다. */
    public static WeekKey of(Instant instant) {
        ZonedDateTime t = instant.atZone(ZONE);
        return new WeekKey(t.get(IsoFields.WEEK_BASED_YEAR), t.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
    }

    public static WeekKey parse(String value) {
        Matcher m = FORMAT.matcher(value);
        if (!m.matches()) throw new InvalidWeekKeyException(value);
        return new WeekKey(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    }

    /** 키 접미사 및 API 표기 형식: yyyy-Www */
    public String value() {
        return String.format("%04d-W%02d", weekBasedYear, week);
    }

    private static int weeksIn(int weekBasedYear) {
        // 12/28 은 항상 그 해의 마지막 ISO 주에 속한다
        return LocalDate.of(weekBasedYear, 12, 28).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }
}
```

#### 예외 (domain/exception)
```java
/** 도메인 불변식 위반. application 이 받아 처리한다 (메시지는 로그에만 남는다). queue·coupon 과 같은 형태 */
public class DomainException extends RuntimeException {
    public DomainException(String message) { super(message); }
}

public class InvalidWeekKeyException extends DomainException {
    public InvalidWeekKeyException(String value) { super("유효하지 않은 주차: " + value); }
}

/**
 * 랭킹 저장소(Redis) 호출 실패 — 연결 실패·명령 타임아웃 등. 순수 Java.
 * DomainException 을 상속하지 않는다: 리스너에서 스킵되지 않고 전파되어 재시도 대상이 되어야 한다.
 */
public class RankingStoreUnavailableException extends RuntimeException {
    public RankingStoreUnavailableException(String message, Throwable cause) { super(message, cause); }
}
```
> 포트가 `domain/repository` 에 있으므로 포트가 던지는 예외도 domain 에 둔다 (coupon 의 `GateUnavailableException` 은 포트가 application 에 있어 application/port 에 있다).

#### `EventFields` — 메시지 필드 검증 (구현 결정)
```java
/** Command 의 박싱 필드를 검증해 원시값으로 꺼낸다. 위반 시 DomainException */
public final class EventFields {
    private EventFields() {}

    public static long id(Long value, String name) {
        if (value == null || value <= 0) throw new DomainException(name + " 가 올바르지 않음: " + value);
        return value;
    }
    public static Instant instant(Instant value, String name) {
        if (value == null) throw new DomainException(name + " 가 없음");
        return value;
    }
    public static String text(String value, String name) {
        if (value == null || value.isBlank()) throw new DomainException(name + " 가 비어 있음");
        return value;
    }
}
```

#### `RankingBoard` — 랭킹 보드 종류
```java
public enum RankingBoard {
    DROP_POPULAR,   // 인기 드로우 (member = dropId)
    USER_WEEKLY,    // 주간 유저 (member = userId, WeekKey 필요)
    USER_ALL        // 누적 유저 (member = userId)
}
```
> 키 문자열(`rank:...`)은 기술 세부사항이므로 domain 에 두지 않고 `infrastructure/redis/RankingKeys` 가 매핑한다.

#### `RankEntry` — 순위 한 줄
```java
/** rank 는 1부터 시작하는 순번(동점이어도 순번은 다름). memberId 는 보드에 따라 dropId 또는 userId. */
public record RankEntry(long rank, long memberId, long score) {}
```

#### `DropWindow` — 드로우 진행 여부 판단 (구현 결정)
```java
public record DropWindow(long dropId, Instant closeAt) {

    /** closeAt 을 아직 모르면(drop.created 미수신) 진행 중으로 간주한다. */
    public boolean isInProgress(Instant now) {
        return closeAt == null || now.isBefore(closeAt);
    }

    /** closeAt 을 알고 이미 지났을 때만 true. 인기 보드에서 lazy 제거 대상 */
    public boolean isClosed(Instant now) {
        return !isInProgress(now);
    }
}
```
> 응모는 오픈 이후에만 들어오므로, 랭킹 ZSet 에 존재하는 드로우는 이미 오픈된 드로우다. 따라서 "진행 중" 판정은 `closeAt` 만 보면 된다. **(확정, 2026-11-17 사용자 결정)**

#### 저장소 포트 (domain/repository)
```java
public interface RankingRepository {

    /**
     * (dropId, userId) 를 처음 보면 3개 보드에 1점씩 더하고 true, 이미 집계했으면 false.
     * dedupe 와 증가는 원자적으로 수행되어야 한다.
     */
    boolean incrementOnce(long dropId, long userId, WeekKey week);

    /** 점수 내림차순 [offset, offset+count). week 는 USER_WEEKLY 일 때만 필수. */
    List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count);

    /** 해당 보드에 없으면 empty. */
    Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId);

    /** 인기 보드에서 종료 드로우 제거 (ZREM). 없는 id 는 무시 — 멱등 */
    void removeDrops(Collection<Long> dropIds);
}

public interface DisplayNameRepository {
    void saveDropTitle(long dropId, String title);
    void saveNickname(long userId, String nickname);
    Map<Long, String> findDropTitles(Collection<Long> dropIds);   // 없는 id 는 맵에 없음
    Map<Long, String> findNicknames(Collection<Long> userIds);
}

public interface DropWindowRepository {                            // (구현 결정)
    void save(long dropId, Instant closeAt);
    Map<Long, DropWindow> findAll(Collection<Long> dropIds);       // 없는 id 는 closeAt=null 로 채움
}
```
- 포트의 id 는 원시 `long` 이다. null 검증은 application 이 Command 에서 끝낸다.
- 세 포트의 Redis 구현은 모두 Redis 예외를 `RankingStoreUnavailableException` 으로 바꿔 던진다 (3.3).

### 3.2 application

#### DTO (application/dto)
```java
/** 메시지 필드를 그대로 담는다 (박싱 타입). 검증은 application 이 한다 */
public record RankingRecordCommand(Long dropId, Long userId, Instant appliedAt) {}
public record DropInfoCommand(Long dropId, String title, Instant closeAt) {}
public record UserInfoCommand(Long userId, String nickname) {}

public record DropRankResult(long rank, long dropId, String title, long entryCount) {}
public record UserRankResult(long rank, long userId, String nickname, long score) {}
public record WeeklyRankingResult(String week, List<UserRankResult> items) {}
public record MyRankResult(String week, Long weeklyRank, long weeklyScore, Long allRank, long allScore) {}
```
> `title`, `nickname` 은 항상 값이 있다. 표시 정보 이벤트가 아직 안 왔으면 application 이 `drop-{id}`, `user-{id}` 로 채운다 ([ranking.md](../../design/05-features/ranking.md) 3장 표시 정보). `weeklyRank`/`allRank` 는 랭킹에 없으면 `null`.

#### 에러 코드 (application/exception)
```java
/** 응답 code 는 {SERVICE}_{REASON}, 상수명은 사유만. 던질 때는 new ApplicationException(RankingErrorCode.X) */
public enum RankingErrorCode implements ErrorCode {
    INVALID_SIZE("RANKING_INVALID_SIZE", 400, "size 는 1 이상 100 이하여야 합니다."),
    INVALID_WEEK("RANKING_INVALID_WEEK", 400, "주차 형식이 올바르지 않습니다. (예: 2026-W47)"),
    TEMPORARILY_FAILED("RANKING_TEMPORARILY_FAILED", 503, "일시적인 오류입니다. 다시 시도해 주세요.");

    private final String code;
    private final int status;
    private final String message;

    RankingErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
```
- `CouponErrorCode` 와 같은 형태. 별도 예외 클래스 없이 `new ApplicationException(RankingErrorCode.X)` 를 던지고, 기존 `GlobalExceptionHandler` 가 `ErrorCode.status()` 로 매핑한다.
- `X-User-Id` 누락은 서비스 코드가 아니라 공통 `COMMON_MISSING_USER_HEADER`(400) 다 (3.4).

#### `RankingAggregateService` — 집계·표시 정보 저장 (쓰기 경로)
```java
@Slf4j
@Service
@RequiredArgsConstructor
public class RankingAggregateService {

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;

    /** @return 이번 호출로 집계됐으면 true, 중복이거나 잘못된 메시지면 false */
    public boolean recordEntry(RankingRecordCommand cmd) {
        long dropId, userId;
        WeekKey week;
        try {
            dropId = EventFields.id(cmd.dropId(), "dropId");
            userId = EventFields.id(cmd.userId(), "userId");
            week = WeekKey.of(EventFields.instant(cmd.appliedAt(), "appliedAt"));   // 처리 시각이 아니라 응모 시각
        } catch (DomainException e) {
            log.warn("invalid entry.created skipped: dropId={} userId={} - {}", cmd.dropId(), cmd.userId(), e.getMessage());
            return false;
        }
        return rankingRepository.incrementOnce(dropId, userId, week);   // RankingStoreUnavailableException 은 전파
    }

    /** 모든 필드를 검증한 뒤에만 쓴다 — 제목만 저장되고 closeAt 이 빠지는 반쪽 저장이 없다 */
    public void registerDrop(DropInfoCommand cmd) {
        long dropId;
        String title;
        Instant closeAt;
        try {
            dropId = EventFields.id(cmd.dropId(), "dropId");
            title = EventFields.text(cmd.title(), "title");
            closeAt = EventFields.instant(cmd.closeAt(), "closeAt");
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", cmd.dropId(), e.getMessage());
            return;
        }
        displayNameRepository.saveDropTitle(dropId, title);
        dropWindowRepository.save(dropId, closeAt);
    }

    public void registerUser(UserInfoCommand cmd) {
        long userId;
        String nickname;
        try {
            userId = EventFields.id(cmd.userId(), "userId");
            nickname = EventFields.text(cmd.nickname(), "nickname");
        } catch (DomainException e) {
            log.warn("invalid user.registered skipped: userId={} - {}", cmd.userId(), e.getMessage());
            return;
        }
        displayNameRepository.saveNickname(userId, nickname);
    }
}
```
> 설계(class-diagram 2.5)에는 `record(RankingRecordCommand)` 만 있으나, 표시 정보 저장도 쓰기 경로이므로 같은 서비스에 `registerDrop`/`registerUser` 를 둔다. **(구현 결정)**
> Redis 만 쓰므로 `@Transactional` 은 붙이지 않는다.
- **메시지 불변식 위반은 재시도해도 결과가 같다** → WARN 후 정상 반환(오프셋 커밋), 저장소를 호출하지 않는다 ([layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.5 리스너 경로).
- **Redis 장애**(`RankingStoreUnavailableException`)는 잡지 않는다 → 리스너 밖으로 전파되어 `KafkaConsumerConfig` 의 재시도 정책을 탄다 (3.3).
- `registerDrop` 의 두 쓰기(HSET 2회)는 원자적이지 않다. 사이에서 Redis 가 실패하면 재시도가 둘 다 다시 쓴다 (HSET 덮어쓰기라 멱등).

#### `RankingQueryService` — 조회 (읽기 경로)
```java
@Slf4j
@Service
@RequiredArgsConstructor
public class RankingQueryService {

    private static final int MAX_SIZE = 100;
    static final int SCAN_BATCH = 50;                   // (구현 결정)
    static final int SCAN_LIMIT = 500;                  // 진행 중 필터 시 최대 탐색 수 (구현 결정)

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;
    private final Clock clock;                          // java.time.Clock Bean (infrastructure/config/ClockConfig)

    /** 진행 중 드로우만 응모 수 순으로 (확정, 2026-11-17 사용자 결정). 탐색 중 본 종료 드로우는 lazy 제거 */
    public List<DropRankResult> topDrops(int size) {
        validateSize(size);
        Instant now = clock.instant();
        List<RankEntry> picked = new ArrayList<>();
        List<Long> closed = new ArrayList<>();
        store(() -> {
            for (long offset = 0; picked.size() < size && offset < SCAN_LIMIT; offset += SCAN_BATCH) {
                List<RankEntry> batch = rankingRepository.top(RankingBoard.DROP_POPULAR, null, offset, SCAN_BATCH);
                Map<Long, DropWindow> windows = dropWindowRepository.findAll(ids(batch));
                for (RankEntry e : batch) {
                    DropWindow w = windows.get(e.memberId());
                    if (w.isClosed(now)) closed.add(e.memberId());
                    else if (picked.size() < size) picked.add(e);
                }
                if (batch.size() < SCAN_BATCH) break;    // 마지막 배치 — 다음 조회 생략
            }
        });
        removeClosed(closed);                            // 탐색이 끝난 뒤에 지운다 (탐색 중 지우면 offset 이 밀린다)
        Map<Long, String> titles = store(() -> displayNameRepository.findDropTitles(ids(picked)));
        // 필터 후 순번을 1부터 다시 매긴다 (구현 결정)
        return IntStream.range(0, picked.size())
                .mapToObj(i -> {
                    long dropId = picked.get(i).memberId();
                    return new DropRankResult(i + 1, dropId,
                            titles.getOrDefault(dropId, "drop-" + dropId), picked.get(i).score());
                })
                .toList();
    }

    /** week 가 null 이면 이번 주 */
    public WeeklyRankingResult weeklyUsers(String week, int size) {
        validateSize(size);
        WeekKey key = (week == null) ? WeekKey.of(clock.instant()) : parseWeek(week);
        List<RankEntry> entries = store(() -> rankingRepository.top(RankingBoard.USER_WEEKLY, key, 0, size));
        return new WeeklyRankingResult(key.value(), withNicknames(entries));
    }

    public List<UserRankResult> allUsers(int size) {
        validateSize(size);
        return withNicknames(store(() -> rankingRepository.top(RankingBoard.USER_ALL, null, 0, size)));
    }

    public MyRankResult myRank(long userId) {
        WeekKey thisWeek = WeekKey.of(clock.instant());
        Optional<RankEntry> weekly = store(() -> rankingRepository.rankOf(RankingBoard.USER_WEEKLY, thisWeek, userId));
        Optional<RankEntry> all = store(() -> rankingRepository.rankOf(RankingBoard.USER_ALL, null, userId));
        return new MyRankResult(thisWeek.value(),
                weekly.map(RankEntry::rank).orElse(null), weekly.map(RankEntry::score).orElse(0L),
                all.map(RankEntry::rank).orElse(null), all.map(RankEntry::score).orElse(0L));
    }

    /** lazy 제거는 부가 작업이다 — 실패해도 조회 결과는 돌려준다 (다음 조회에서 다시 시도) */
    private void removeClosed(List<Long> closed) {
        if (closed.isEmpty()) return;
        try {
            rankingRepository.removeDrops(closed);
        } catch (RankingStoreUnavailableException e) {
            log.warn("closed drop lazy removal failed: count={} - {}", closed.size(), e.getMessage());
        }
    }

    /** Redis 장애 → 503 (E-03, timeout 1s) */
    private <T> T store(Supplier<T> call) {
        try {
            return call.get();
        } catch (RankingStoreUnavailableException e) {
            throw new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED, e);
        }
    }
    private void store(Runnable call) { store(() -> { call.run(); return null; }); }

    private WeekKey parseWeek(String week) {
        try { return WeekKey.parse(week); }
        catch (InvalidWeekKeyException e) { throw new ApplicationException(RankingErrorCode.INVALID_WEEK); }
    }

    private void validateSize(int size) {
        if (size < 1 || size > MAX_SIZE) throw new ApplicationException(RankingErrorCode.INVALID_SIZE);
    }
    // ids(), withNicknames() — 닉네임 없으면 "user-" + userId, findNicknames 도 store() 로 감싼다
}
```
- public 메서드의 파라미터·반환은 원시값·String·Result 뿐이다 (ArchUnit R6). `RankEntry`, `WeekKey` 는 내부에서만 쓴다.
- `Clock` 주입으로 테스트에서 시각을 고정한다.
- **스캔 조기 종료**: 배치가 `SCAN_BATCH` 보다 작으면 보드 끝이므로 더 조회하지 않는다 (빈 배치 왕복 1회 절약).
- **SCAN_LIMIT 경계**: 탐색은 상위 500개(offset 0~499)까지다. 500번째의 진행 중 드로우는 찾고, 501번째는 못 찾는다 (T7-29).
- **종료 드로우 lazy 제거 (확정(기본안), 2026-11-17)**: 탐색 중 `closeAt` 을 알고 이미 지난 드로우를 모아 탐색이 끝난 뒤 `ZREM rank:drop:popular` 한다.
  - 읽기 경로의 쓰기다. ZREM 은 멱등이라 동시 조회가 같은 id 를 지워도 문제없다.
  - 종료 드로우는 인기 보드에서 **영구히 빠진다** (진행 중만 보여주는 정책이므로 손실 없음). `closeAt` 을 모르는(`drop.created` 미수신) 드로우는 지우지 않는다.
  - 마감 직후 늦게 도착한 `entry.created` 가 ZINCRBY 로 다시 넣을 수 있다 → 다음 조회에서 다시 지워진다 (허용).
  - 효과: 종료 드로우가 상위를 채워 진행 중 드로우가 500 밖으로 밀리는 일을 막는다. 한 번의 조회로 지울 수 있는 수는 SCAN_LIMIT 까지다.

### 3.3 infrastructure

#### `RankingKeys` — 키 이름 매핑
```java
final class RankingKeys {
    static final String DROP_POPULAR  = "rank:drop:popular";
    static final String USER_ALL      = "rank:user:all";
    static final String DROP_INFO     = "rank:drop:info";
    static final String USER_INFO     = "rank:user:info";
    static final String DROP_CLOSE_AT = "rank:drop:closeAt";   // (구현 결정)

    static String weekly(WeekKey week) { return "rank:user:weekly:" + week.value(); }
    static String dedupe(long dropId)  { return "rank:dedupe:" + dropId; }

    static String board(RankingBoard board, WeekKey week) {
        return switch (board) {
            case DROP_POPULAR -> DROP_POPULAR;
            case USER_ALL     -> USER_ALL;
            case USER_WEEKLY  -> weekly(Objects.requireNonNull(week, "USER_WEEKLY 는 week 필수"));
        };
    }
}
```

#### `rank.lua` — dedupe + 3개 보드 증가 (원자)
경로: `ranking-service/src/main/resources/scripts/rank.lua`

| 구분 | 인덱스 | 값 |
|------|--------|-----|
| KEYS | 1 | `rank:dedupe:{dropId}` |
| | 2 | `rank:drop:popular` |
| | 3 | `rank:user:weekly:{yyyy-Www}` |
| | 4 | `rank:user:all` |
| | 5 | `rank:drop:closeAt` |
| ARGV | 1 | dropId |
| | 2 | userId |
| | 3 | 주간 키 TTL (초) — 기본 3,024,000 (35일) |
| | 4 | dedupe 유예 (ms) — closeAt 이후 보관 기간, 기본 7일 |
| | 5 | dedupe 기본 TTL (ms) — closeAt 을 모를 때, 기본 30일 |
| | 6 | dedupe 최소 TTL (ms) — 기본 1일 |
| | 7 | 현재 시각 (epoch ms) — 애플리케이션 시각 (Lua `TIME` 미사용, [entry-apply.md](../../design/05-features/entry-apply.md) 와 같은 이유) |
| 반환 | | `1` = 집계함, `0` = 중복(스킵) |

```lua
-- rank.lua : (dropId, userId) 를 처음 볼 때만 3개 보드에 1점씩 더한다
-- 읽기·계산을 모두 끝낸 뒤 마지막에 쓴다: 계산 중 런타임 오류가 나도 dedupe 만 남고 점수가 빠지는 일이 없다
local dropId = ARGV[1]
local userId = ARGV[2]

-- 1) 멱등 처리: 이미 집계한 응모면 아무것도 하지 않는다
if redis.call('SISMEMBER', KEYS[1], userId) == 1 then
    return 0
end

-- 2) 계산 (쓰기 없음)
local now = tonumber(ARGV[7])
local weeklyTtl = tonumber(ARGV[3])
--    dedupe Set 만료 시각: closeAt + 유예. 단, 최소 TTL 보장
--    (재처리 시 closeAt 이 이미 지났더라도 키가 즉시 삭제되지 않도록). 키가 없으면(-2) 처음 만드는 것
local dedupeExpireAt = nil
if redis.call('PTTL', KEYS[1]) < 0 then
    local closeAt = redis.call('HGET', KEYS[5], dropId)
    if closeAt then
        dedupeExpireAt = tonumber(closeAt) + tonumber(ARGV[4])
    else
        dedupeExpireAt = now + tonumber(ARGV[5])
    end
    local minExpireAt = now + tonumber(ARGV[6])
    if dedupeExpireAt < minExpireAt then dedupeExpireAt = minExpireAt end
end
--    주간 키 TTL 은 처음 만들 때(-2) 또는 TTL 이 없을 때(-1)만 설정 — 연장하지 않는다
local weeklyNeedsTtl = redis.call('TTL', KEYS[3]) < 0

-- 3) 쓰기 (모두 끝에)
redis.call('SADD', KEYS[1], userId)
if dedupeExpireAt then redis.call('PEXPIREAT', KEYS[1], dedupeExpireAt) end
redis.call('ZINCRBY', KEYS[2], 1, dropId)
redis.call('ZINCRBY', KEYS[3], 1, userId)
if weeklyNeedsTtl then redis.call('EXPIRE', KEYS[3], weeklyTtl) end
redis.call('ZINCRBY', KEYS[4], 1, userId)

return 1
```
- 모든 명령이 O(1)/O(log N) 이라 스크립트가 Redis 를 오래 붙잡지 않는다.
- Redis Lua 는 롤백이 없다. `tonumber` 등 실패할 수 있는 계산을 쓰기 앞에 두어, 오류가 나면 아무것도 쓰지 않은 상태로 끝나게 한다 (재시도 시 정상 집계).
- `PEXPIREAT` 는 **Redis 서버 시각** 기준 절대 시각이다. ARGV[7](앱 시각)이 서버 시각과 크게 다르면 키가 바로 만료될 수 있다 → 테스트는 실제 시각 근처 Clock 을 쓴다 (4장).
- 주간 키 TTL 은 **그 주의 첫 기록 시점**부터 35일. 지난 주차 랭킹을 약 4~5주 조회할 수 있다.
- 서로 다른 키 5개를 한 스크립트에서 쓴다. 단일 Redis 에서는 문제없지만 Redis Cluster 로 가면 슬롯이 달라 실패한다 → 6장 리스크.

#### `RankingProperties`
```java
@ConfigurationProperties(prefix = "ranking")
public record RankingProperties(
        Duration weeklyTtl,          // 기본 35d
        Duration dedupeGrace,        // 기본 7d
        Duration dedupeFallbackTtl,  // 기본 30d
        Duration dedupeMinTtl        // 기본 1d
) {}
```
`application.yml` 추가분:
```yaml
spring:
  data:
    redis:
      timeout: 1s                # 명령 타임아웃 (Lettuce 기본 60초, E-03). 조회가 Redis 장애에 오래 묶이지 않게 → 503
  kafka:
    consumer:
      enable-auto-commit: false  # 처리 성공 후 컨테이너가 커밋 (ack-mode record)
    listener:
      ack-mode: record           # 레코드 단위로 처리 성공 후 커밋 (coupon 과 같음, KafkaConsumption 이 커밋 오프셋을 본다)

ranking:
  weekly-ttl: ${RANKING_WEEKLY_TTL:35d}
  dedupe-grace: ${RANKING_DEDUPE_GRACE:7d}
  dedupe-fallback-ttl: ${RANKING_DEDUPE_FALLBACK_TTL:30d}
  dedupe-min-ttl: ${RANKING_DEDUPE_MIN_TTL:1d}
```
> 타임아웃 1초가 나면 Lua 가 이미 실행됐을 수 있다(애매한 실패). 리스너 재시도는 dedupe 로 멱등이라 이중 집계가 없고, 조회는 읽기라 부작용이 없다.

#### `RedisScriptConfig`, `ClockConfig`
```java
/** 빈 이름 = RedisRankingRepository 필드 이름 (coupon 과 같은 형태) */
@Configuration
@EnableConfigurationProperties(RankingProperties.class)
public class RedisScriptConfig {

    @Bean
    RedisScript<Long> rankScript() {
        return RedisScript.of(new ClassPathResource("scripts/rank.lua"), Long.class);
    }
}

@Configuration
public class ClockConfig {

    @Bean
    Clock clock() { return Clock.systemUTC(); }   // 주차 계산은 WeekKey 가 Asia/Seoul 로 변환
}
```
> 테스트는 `@TestConfiguration` 의 `@Primary Clock` 빈으로 바꾼다 (다른 서비스와 같음).

#### `RedisRankingRepository` — `RankingRepository` 구현
```java
@Repository
@RequiredArgsConstructor
class RedisRankingRepository implements RankingRepository {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> rankScript;          // 빈 이름 = 필드 이름 (RedisScriptConfig)
    private final RankingProperties props;
    private final Clock clock;

    @Override
    public boolean incrementOnce(long dropId, long userId, WeekKey week) {
        List<String> keys = List.of(
                RankingKeys.dedupe(dropId), RankingKeys.DROP_POPULAR, RankingKeys.weekly(week),
                RankingKeys.USER_ALL, RankingKeys.DROP_CLOSE_AT);
        Long result = call(() -> redis.execute(rankScript, keys,
                String.valueOf(dropId), String.valueOf(userId),
                String.valueOf(props.weeklyTtl().toSeconds()),
                String.valueOf(props.dedupeGrace().toMillis()),
                String.valueOf(props.dedupeFallbackTtl().toMillis()),
                String.valueOf(props.dedupeMinTtl().toMillis()),
                String.valueOf(clock.millis())));
        return Long.valueOf(1L).equals(result);
    }

    @Override
    public List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count) {
        Set<ZSetOperations.TypedTuple<String>> tuples = call(() -> redis.opsForZSet()
                .reverseRangeWithScores(RankingKeys.board(board, week), offset, offset + count - 1));
        // tuples 는 점수 내림차순. rank = offset + index + 1
        // ...
    }

    @Override
    public Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId) {
        String key = RankingKeys.board(board, week);
        Long rank = call(() -> redis.opsForZSet().reverseRank(key, String.valueOf(memberId)));
        if (rank == null) return Optional.empty();
        Double score = call(() -> redis.opsForZSet().score(key, String.valueOf(memberId)));
        if (score == null) return Optional.empty();          // 두 호출 사이에 제거된 경우
        return Optional.of(new RankEntry(rank + 1, memberId, score.longValue()));
    }

    @Override
    public void removeDrops(Collection<Long> dropIds) {
        if (dropIds.isEmpty()) return;
        call(() -> redis.opsForZSet().remove(RankingKeys.DROP_POPULAR,
                dropIds.stream().map(String::valueOf).toArray()));
    }

    /** Redis 예외(연결 실패·명령 타임아웃 = QueryTimeoutException 등 DataAccessException 계열)를 포트 예외로 (coupon RedisCouponGate 와 같은 형태) */
    private static <T> T call(Supplier<T> redisCall) {
        try {
            return redisCall.get();
        } catch (DataAccessException e) {
            throw new RankingStoreUnavailableException("Redis 랭킹 저장소 호출 실패", e);
        }
    }
}
```
> `ZREVRANK` 와 `ZSCORE` 사이에 점수가 바뀔 수 있지만, 조회 결과의 순간 불일치는 랭킹 특성상 허용한다.

#### `RedisDisplayNameRepository`, `RedisDropWindowRepository`
```java
@Repository
@RequiredArgsConstructor
class RedisDisplayNameRepository implements DisplayNameRepository {
    private final StringRedisTemplate redis;

    @Override public void saveDropTitle(long dropId, String title) {
        redis.opsForHash().put(RankingKeys.DROP_INFO, String.valueOf(dropId), title);
    }
    @Override public void saveNickname(long userId, String nickname) {
        redis.opsForHash().put(RankingKeys.USER_INFO, String.valueOf(userId), nickname);
    }
    @Override public Map<Long, String> findDropTitles(Collection<Long> ids) {
        return multiGet(RankingKeys.DROP_INFO, ids);   // HMGET, null 값은 제외
    }
    // findNicknames ... (동일 패턴)
}

@Repository
@RequiredArgsConstructor
class RedisDropWindowRepository implements DropWindowRepository {        // (구현 결정)
    private final StringRedisTemplate redis;

    @Override public void save(long dropId, Instant closeAt) {
        redis.opsForHash().put(RankingKeys.DROP_CLOSE_AT, String.valueOf(dropId),
                String.valueOf(closeAt.toEpochMilli()));
    }
    @Override public Map<Long, DropWindow> findAll(Collection<Long> dropIds) {
        // HMGET rank:drop:closeAt ... → 없으면 new DropWindow(id, null)
    }
}
```
- 같은 `drop.created`/`user.registered` 가 재전달되어도 `HSET` 은 덮어쓰기라 멱등이다.
- 두 구현도 `RedisRankingRepository` 와 같은 `call()` 래퍼로 Redis 예외를 `RankingStoreUnavailableException` 으로 바꾼다.

#### `KafkaConsumerConfig` — 리스너 에러 처리
```java
/**
 * Redis 만 쓰는 컨슈머 실패 정책 (architecture.md 4장, queue KafkaConsumerConfig 와 같음): 1초 간격 3회 재시도 후
 * 로그를 남기고 건너뛴다. DLT 없음 (확정, 2026-11-16 — 랭킹은 응모의 진실의 원천이 아니고 재집계로 복구).
 * Spring Boot 가 이 CommonErrorHandler 빈 하나를 리스너 컨테이너 팩토리에 자동 적용한다.
 * 역직렬화 실패(DeserializationException)는 기본으로 재시도하지 않는 예외라 바로 recoverer 로 간다 (파티션을 막지 않음).
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    static final long RETRY_INTERVAL_MS = 1_000L;
    static final long MAX_RETRIES = 3L;

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        ConsumerRecordRecoverer recoverer = (record, ex) ->
                log.error("ranking 집계 실패 — 건너뜀 topic={} partition={} offset={} key={}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
    }
}
```
> 역직렬화 설정(JSON → common 이벤트 record, `ErrorHandlingDeserializer`)은 0단계 공통 설정을 그대로 쓴다.
> 메시지 불변식 위반은 application 이 WARN 후 정상 반환하므로 이 핸들러까지 오지 않는다 (재시도 없음). 여기로 오는 것은 Redis 장애와 역직렬화 실패뿐이다.

### 3.4 presentation

#### Listener 3개 (presentation/listener)
메시지(common 이벤트) → Command 변환 후 application 호출. application 은 common 메시지를 모른다.

```java
@Component
@RequiredArgsConstructor
class EntryCreatedListener {
    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.ENTRY_CREATED)
    void on(EntryCreated message) {
        aggregateService.recordEntry(
                new RankingRecordCommand(message.dropId(), message.userId(), message.appliedAt()));
    }
}

@Component
@RequiredArgsConstructor
class DropCreatedListener {
    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.DROP_CREATED)
    void on(DropCreated message) {
        aggregateService.registerDrop(
                new DropInfoCommand(message.dropId(), message.title(), message.closeAt()));
    }
}

@Component
@RequiredArgsConstructor
class UserRegisteredListener {
    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.USER_REGISTERED)
    void on(UserRegistered message) {
        aggregateService.registerUser(new UserInfoCommand(message.userId(), message.nickname()));
    }
}
```
- group-id 는 `application.yml` 의 `ranking-service` 를 공유한다. 토픽이 달라 서로 간섭하지 않는다.
- 리스너는 필드를 그대로 Command 에 옮긴다 (null 검사 없음). 검증·스킵은 application 의 몫이다 (3.2).
- `drop.created` 보다 `entry.created` 가 먼저 와도 문제없다: 제목은 조회 시 `drop-{id}`, dedupe TTL 은 기본 TTL(30일)이 적용된다.

#### `RankingController` — API 4개
```java
@RestController
@RequestMapping("/api/rankings")
@RequiredArgsConstructor
class RankingController {
    private final RankingQueryService queryService;

    @GetMapping("/drops")
    List<DropRankingResponse> drops(@RequestParam(defaultValue = "10") int size) {
        return queryService.topDrops(size).stream().map(DropRankingResponse::from).toList();
    }

    @GetMapping("/users/weekly")
    UserRankingResponse weekly(@RequestParam(required = false) String week,
                               @RequestParam(defaultValue = "10") int size) {
        return UserRankingResponse.from(queryService.weeklyUsers(week, size));
    }

    @GetMapping("/users/all")
    UserRankingResponse all(@RequestParam(defaultValue = "10") int size) {
        return UserRankingResponse.ofAll(queryService.allUsers(size));
    }

    /** 헤더 누락(Gateway 우회 등)은 MissingRequestHeaderException → GlobalExceptionHandler 가 400 COMMON_MISSING_USER_HEADER */
    @GetMapping("/users/me")
    MyRankResponse me(@RequestHeader("X-User-Id") Long userId) {
        return MyRankResponse.from(queryService.myRank(userId));
    }
}
```

| Method | Path | 인증 | 응답 예시 |
|--------|------|------|-----------|
| GET | `/api/rankings/drops?size=10` | 공개 | `[{"rank":1,"dropId":3,"title":"…","entryCount":1000}]` |
| GET | `/api/rankings/users/weekly?week=2026-W47&size=10` | 공개 | `{"week":"2026-W47","items":[{"rank":1,"userId":7,"nickname":"…","score":5}]}` |
| GET | `/api/rankings/users/all?size=10` | 공개 | `{"week":null,"items":[…]}` |
| GET | `/api/rankings/users/me` | **USER** | `{"week":"2026-W47","weeklyRank":12,"weeklyScore":3,"allRank":40,"allScore":9}` |

Response record (presentation/api/response) — Result 에서만 변환하고 domain 을 import 하지 않는다:
```java
public record DropRankingResponse(long rank, long dropId, String title, long entryCount) {
    public static DropRankingResponse from(DropRankResult r) { return new DropRankingResponse(r.rank(), r.dropId(), r.title(), r.entryCount()); }
}
public record UserRankingResponse(String week, List<Item> items) {
    public record Item(long rank, long userId, String nickname, long score) {}
    public static UserRankingResponse from(WeeklyRankingResult r) { /* ... */ }
    public static UserRankingResponse ofAll(List<UserRankResult> r) { /* week = null */ }
}
public record MyRankResponse(String week, Long weeklyRank, long weeklyScore, Long allRank, long allScore) {
    public static MyRankResponse from(MyRankResult r) { /* ... */ }
}
```

#### 에러 응답 (기존 `GlobalExceptionHandler`)
| 원인 | code | HTTP |
|------|------|------|
| size 범위 위반 | `RANKING_INVALID_SIZE` | 400 |
| week 형식·존재하지 않는 주차 | `RANKING_INVALID_WEEK` | 400 |
| Redis 장애·타임아웃 | `RANKING_TEMPORARILY_FAILED` | 503 |
| `/users/me` 에 `X-User-Id` 없음 | `COMMON_MISSING_USER_HEADER` | 400 |

응답 바디는 0단계 공통 에러 형식(`ErrorResponse`)을 따른다. 랭킹 전용 `@ExceptionHandler` 는 두지 않는다. 정상 경로에서는 Gateway 가 `/users/me` 에 JWT 를 요구하므로(401 은 Gateway 응답) 서비스까지 헤더 없이 오는 것은 우회뿐이다.

### 3.5 동점 처리 (확정, 2026-11-17 사용자 결정)
- Redis ZSet 은 같은 점수일 때 member **문자열 사전순**으로 정렬하고, `ZREVRANGE` 는 그 역순(사전 역순)으로 돌려준다.
  - 예: 점수 5 인 userId `"10"`, `"9"` → `ZREVRANGE` 결과 `"9"`, `"10"` 순.
- 순번(`rank`)은 동점이어도 서로 다른 순번(1, 2, …)을 매긴다. "공동 1위" 표기는 하지 않는다.
- "먼저 달성한 사람 우선" 같은 정책이 필요해지면 score 에 시간 보정값을 섞는 방식으로 바꾼다 (6장).

### 3.6 운영 절차 — Kafka 오프셋 리셋으로 재집계
Redis 유실 또는 집계 버그 수정 후 랭킹을 다시 만드는 절차. `entry.created` 보존 기간(30일, 0단계 설정) 안의 응모만 복구된다.

```bash
# 1) ranking-service 중지 (컨슈머 그룹이 비활성이어야 오프셋 리셋 가능)
docker compose stop ranking-service            # 8단계 이전엔 IDE 에서 중지

# 2) 집계 키 삭제 (표시 정보 rank:drop:info / rank:user:info / rank:drop:closeAt 은 유지)
for p in 'rank:drop:popular' 'rank:user:all' 'rank:user:weekly:*' 'rank:dedupe:*'; do
  docker compose exec -T redis redis-cli --scan --pattern "$p" \
    | xargs -r docker compose exec -T redis redis-cli DEL
done

# 3) entry.created 오프셋만 처음으로 리셋 (dry-run 후 execute)
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group ranking-service \
  --topic entry.created --reset-offsets --to-earliest --dry-run
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group ranking-service \
  --topic entry.created --reset-offsets --to-earliest --execute

# 4) ranking-service 재시작 → 처음부터 재소비
docker compose start ranking-service
```
- **dedupe 키를 반드시 함께 지운다.** 남겨두면 재소비 이벤트가 모두 "중복"으로 스킵되어 점수가 0 이 된다.
- 반대로 dedupe 가 만료된(closeAt + 7일 경과) 상태에서 집계 키를 지우지 않고 오프셋만 리셋하면 **이중 집계**된다. 그래서 2)와 3)을 항상 한 세트로 실행한다.
- 표시 정보까지 유실된 경우: `drop.created`/`user.registered` 보존 기간(기본 7일) 밖이면 복구되지 않는다. 드로우는 product 의 재발행 API 로 복구 가능, 닉네임은 복구 수단 없음 → 6장 리스크.
- `KEYS` 대신 `--scan` 을 쓴다 (운영 중 Redis 블로킹 방지).
- 재소비로 종료 드로우도 `rank:drop:popular` 에 다시 들어온다. 첫 인기 드로우 조회의 lazy 제거(3.2)가 다시 지운다.

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 |
|----|-----------|---------------|-------------------------------|------|
| T7-01 | domain 단위 | `WeekKeyTest` | Given `2026-11-22T23:59:59+09:00`(일) / When `of` / Then `2026-W47` | REQ-RANK-02 |
| T7-02 | domain 단위 | `WeekKeyTest` | Given `2026-11-23T00:00:00+09:00`(월) / When `of` / Then `2026-W48` | REQ-RANK-02 |
| T7-03 | domain 단위 | `WeekKeyTest` | Given `2026-11-22T15:00:00Z` (UTC 로는 일요일, KST 로는 월요일 00:00) / When `of` / Then `2026-W48` (UTC 기준이면 W47 — 다르게 나와야 함) | REQ-RANK-02 |
| T7-04 | domain 단위 | `WeekKeyTest` | Given `2027-01-01T00:00:00+09:00` / When `of` / Then `2026-W53` (주 기준 연도가 달력 연도와 다름) | REQ-RANK-02 |
| T7-05 | domain 단위 | `WeekKeyTest` | Given `"2026-W53"` / When `parse` / Then 성공 · Given `"2025-W53"`, `"2026-W54"`, `"2026-47"`, `"2026-W7"` / Then `InvalidWeekKeyException` | REQ-RANK-02 |
| T7-06 | domain 단위 | `WeekKeyTest` | Given `parse("2026-W05")` / When `value()` / Then `"2026-W05"` (왕복 일치) | REQ-RANK-02 |
| T7-07 | domain 단위 | `DropWindowTest` | closeAt 직전 → 진행 중, closeAt 정각 → 종료, closeAt null → 진행 중 | REQ-RANK-01 |
| T7-08 | application 단위 (Fake) | `RankingAggregateServiceTest` | Given appliedAt=지난주 일요일 23:59 KST, 현재=이번 주 / When `recordEntry` / Then 지난주 키에 집계 (처리 시각이 아닌 이벤트 시각) | REQ-RANK-02, 03 |
| T7-09 | application 단위 (Fake) | `RankingQueryServiceTest` | Given 인기 순위 1~3위 중 2위가 종료된 드로우 / When `topDrops(2)` / Then 1위·3위 반환, 순번 1·2 로 재부여 | REQ-RANK-01 |
| T7-10 | application 단위 (Fake) | `RankingQueryServiceTest` | Given 상위 60개가 모두 종료 드로우, 61번째가 진행 중 / When `topDrops(1)` / Then 다음 배치까지 탐색해 61번째 반환 | REQ-RANK-01 |
| T7-11 | application 단위 (Fake) | `RankingQueryServiceTest` | size 0, 101 → `ApplicationException(INVALID_SIZE)` · week `"abc"` → `ApplicationException(INVALID_WEEK)` · week null → 현재 시각(Clock 고정)의 주차 | REQ-RANK-02 |
| T7-12 | application 단위 (Fake) | `RankingQueryServiceTest` | Given 랭킹에 없는 사용자 / When `myRank` / Then rank null, score 0 | REQ-RANK-02 |
| T7-13 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | Given 같은 (dropId, userId) / When `incrementOnce` 3번 / Then 첫 호출만 true, 3개 보드 점수 모두 1 | REQ-RANK-03 |
| T7-14 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | Given 같은 드로우에 사용자 2명 / When 각 1회 / Then popular=2, 각 사용자 weekly=1, all=1 | REQ-RANK-01, 02 |
| T7-15 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | 첫 기록 후 weekly 키 TTL ≈ 35일(범위 검증), 두 번째 기록 후에도 TTL 이 연장되지 않음 | REQ-RANK-02 |
| T7-16 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | dedupe TTL(`PTTL` 범위로 검증): closeAt 있음 → closeAt+7일 / closeAt 없음 → now+30일 / closeAt 이 10일 전 → now+1일 (최소 TTL). Clock 은 실제 시각 근처(`Clock.offset(Clock.systemUTC(), …)`) — `PEXPIREAT` 이 Redis 서버 시각 기준이라 고정 과거 시각이면 키가 바로 만료된다 | REQ-RANK-03 |
| T7-17 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | 동점 2명 → `ZREVRANGE` 사전 역순, 순번 1·2 | (동점 정책) |
| T7-18 | infra 통합 (Redis) | `RedisDisplayNameRepositoryTest` | 제목·닉네임 저장 후 일괄 조회, 없는 id 는 맵에서 제외, 같은 값 재저장 멱등 | REQ-RANK-01, 02 |
| T7-19 | presentation 슬라이스 | `RankingControllerTest` | `/users/me` 헤더 없음 → 400 `COMMON_MISSING_USER_HEADER` · `?size=0` → 400 `RANKING_INVALID_SIZE` · `?week=2026-W99` → 400 `RANKING_INVALID_WEEK` · 정상 응답 JSON 필드 확인 | REQ-RANK-01, 02 |
| T7-20 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | Given `user.registered`, `drop.created`, `entry.created` 발행 / When 소비 완료(await) / Then `/api/rankings/drops` 에 제목, `/users/all` 에 닉네임 포함 · 표시 정보가 없는 id 는 `drop-{id}`, `user-{id}` | REQ-RANK-01~03 |
| T7-21 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | 같은 `entry.created` 메시지 3번 발행 → **3건 모두 커밋될 때까지 대기**(`KafkaConsumption.fullyConsumed`, group `ranking-service`, lag 0) 후 점수 1. 점수만 기다리면 1번째 처리 직후 통과해 중복 처리를 검증하지 못한다 | REQ-RANK-03 |
| T7-22 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | `entry.created` 가 `drop.created` 보다 먼저 도착 → 집계 정상, 제목 `drop-{id}`, 이후 `drop.created` 도착 시 제목 채워짐 | REQ-RANK-03 |
| T7-23 | 아키텍처 | `ArchitectureTest` | R1~R7 통과 (특히 presentation → domain 의존 없음, application 시그니처에 `WeekKey`/`RankEntry` 없음) | layered |
| T7-24 | 운영 절차 (수동) | 3.6 절차 | 응모 50건 집계 → 집계 키 삭제·오프셋 리셋·재시작 → 점수 동일하게 복구 | NFR-03 |
| T7-25 | 통합 (Kafka + Redis) | `RankingListenerRetryIntegrationTest` | Given `RankingRepository` 를 `@MockitoBean` 으로 바꿔 dropId A 는 항상 `RankingStoreUnavailableException` / When A, B 순서로 발행(파티션 1개) / Then A 는 1초 간격 총 4회 호출(첫 호출~마지막 ≥ 2.5초) 후 건너뜀, B 는 1회 처리 (DLT 없음) | REQ-RANK-03, architecture 4장 |
| T7-26 | 통합 (Kafka + Redis) | `RankingListenerRetryIntegrationTest` | Given JSON 이 아닌 바이트를 `entry.created` 에 발행(별도 bytes 프로듀서) 후 정상 메시지 발행 / Then 깨진 메시지는 재시도 없이 recoverer 로 건너뛰고 정상 메시지가 집계됨 (파티션이 막히지 않음) | REQ-RANK-03 |
| T7-27 | application 단위 (Fake) + presentation 슬라이스 | `RankingQueryServiceTest`, `RankingControllerTest` | Given Fake 저장소가 `RankingStoreUnavailableException` / When `topDrops`·`weeklyUsers`·`allUsers`·`myRank` / Then `ApplicationException(TEMPORARILY_FAILED)` · 컨트롤러는 503 `RANKING_TEMPORARILY_FAILED` | E-03 |
| T7-28 | application 단위 (Fake) | `RankingAggregateServiceTest` | Given null 또는 0 이하 id, null `appliedAt`/`closeAt`, 빈 `title`/`nickname` / When `recordEntry`·`registerDrop`·`registerUser` / Then 예외 없이 반환(WARN), 저장소 호출 0회. `registerDrop` 에서 closeAt 만 null 이어도 제목도 저장하지 않음 | layered 4.5 |
| T7-29 | application 단위 (Fake) | `RankingQueryServiceTest` | Given 상위 499개가 종료, 500번째가 진행 중 / When `topDrops(1)` / Then 500번째 반환 · Given 상위 500개가 종료, 501번째가 진행 중 / Then 빈 목록 (SCAN_LIMIT 경계) · 보드가 50개 미만이면 `top` 1회만 호출 (조기 종료) | REQ-RANK-01 |
| T7-30 | application 단위 (Fake) + infra 통합 (Redis) | `RankingQueryServiceTest`, `RedisRankingRepositoryTest` | Given 종료 드로우 3개(closeAt 지남)·closeAt 미상 1개·진행 중 1개 / When `topDrops` / Then 종료 3개만 `removeDrops`, 미상은 남음, 두 번째 조회에서는 제거 대상 없음 · `removeDrops` 실패해도 조회 결과 반환 · Redis: `removeDrops` 후 ZSet 에서 빠지고 없는 id 는 무시 | REQ-RANK-01 |

> 통합 테스트의 비동기 검증은 Awaitility(`await().atMost(10s)`)를 쓴다 (0단계 testFixtures 에 포함되지 않았다면 이 단계에서 추가 — 구현 결정).

**테스트 격리 규칙** (4~6단계에서 정한 방식)
- **Kafka 통합 테스트**(`RankingListener*IntegrationTest`): 토픽 자동 생성이 꺼져 있으므로 `@TestConfiguration` 의 `NewTopic` 빈(또는 `KafkaAdmin.createOrModifyTopics`)으로 `entry.created`·`drop.created`·`user.registered` 를 만들고, 첫 발행 전에 `KafkaTopicReadiness.awaitTopicMetadata` 로 기다린다.
- **커밋 확인**: "N 번 처리됐다"를 검증할 때는 결과 값만 기다리지 않고 `support/KafkaConsumption.fullyConsumed(kafkaAdmin, topic)`(coupon 과 같은 형태, `APP_GROUP = "ranking-service"`)으로 lag 0 까지 기다린다 (T7-21, T7-26).
- **Kafka 컨테이너가 없는 테스트**(`Redis*RepositoryTest` 등): `@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")` + `@Import(RedisContainerConfig.class)` 또는 Redis 슬라이스(`@DataRedisTest`)로 리스너를 띄우지 않는다. 켜 두면 기본 bootstrap(`localhost:9092`)인 로컬 개발 Kafka 에 붙어 소비·오프셋 커밋할 수 있다.
- **`ApplicationContextTest`**: entry·coupon 과 같이 `spring.kafka.listener.auto-startup=false` 를 추가한다 (Kafka 컨테이너는 그대로, 리스너만 끈다 — 이 단계부터 리스너가 생기므로).
- **Clock**: 테스트는 `@Primary` 테스트 Clock 빈으로 바꾼다. Redis TTL 을 보는 테스트는 실제 시각 근처 Clock 을 쓰고 TTL 은 `PTTL` 범위로 검증한다 (T7-15, T7-16).
- Gradle 테스트는 동시에 두 개 돌리지 않는다 (macOS 포트 충돌).

---

## 5. 커밋 계획

| 순서 | 커밋 메시지 | 포함 |
|------|-------------|------|
| 0 | `docs: 7단계 착수 전 결정 반영`<br>- 사용자 결정 2건(인기 드로우 = 진행 중만, 동점 = member 사전순)<br>- 에러 코드·헤더·Redis 타임아웃·실패 정책·표시 fallback·lazy 제거를 4~6단계 계약에 맞춤 | 이 문서 v1.2, ranking.md, architecture.md, class-diagram.md, implementation-plan.md, enhancement-backlog.md (E-03·E-18) |
| 1 | `feat(ranking): 주차 값 객체와 랭킹 도메인 모델 추가`<br>- WeekKey (Asia/Seoul ISO 주차, 파싱·검증)<br>- RankingBoard, RankEntry, DropWindow, EventFields<br>- DomainException, InvalidWeekKeyException, RankingStoreUnavailableException<br>- 저장소 포트 3개 | domain 전체, T7-01~07 |
| 2 | `feat(ranking): 랭킹 집계·조회 유스케이스 추가`<br>- RankingAggregateService (필드 검증 후 스킵), RankingQueryService (진행 중 필터, lazy 제거, 503 변환)<br>- Command/Result DTO, RankingErrorCode<br>- Fake 저장소 기반 단위 테스트 | application 전체, T7-08~12, T7-27(application), T7-28, T7-29, T7-30(application) |
| 3 | `feat(ranking): Redis 랭킹 저장소와 집계 Lua 스크립트 구현`<br>- rank.lua (SISMEMBER·계산 후 SADD + ZINCRBY 3개, TTL 처리)<br>- RedisRankingRepository, RedisDisplayNameRepository, RedisDropWindowRepository (`call()` 예외 변환)<br>- RankingProperties, RedisScriptConfig, ClockConfig, Redis timeout 1s | infra redis·config, `application.yml`, T7-13~18, T7-30(Redis) |
| 4 | `feat(ranking): 랭킹 이벤트 리스너 추가`<br>- entry.created / drop.created / user.registered 소비<br>- 재시도 후 건너뛰는 에러 핸들러 (KafkaConsumerConfig), listener ack-mode record | listener, `KafkaConsumerConfig`, `support/KafkaConsumption`, `ApplicationContextTest` auto-startup=false, T7-20~22, T7-25, T7-26 |
| 5 | `feat(ranking): 랭킹 조회 API 추가`<br>- 인기 드로우, 주간·누적 유저, 내 순위 (`X-User-Id` 필수) | controller, response, T7-19, T7-27(controller) |
| 6 | `docs: 7단계 구현 결과 반영`<br>- 진행 현황 갱신과 남은 문서 | 플랜 진행 현황, 구현 중 바뀐 부분의 설계 문서 |

---

## 6. 결정 필요 · 리스크

### 결정 필요
| 항목 | 기본안 | 비고 |
|------|--------|------|
| 인기 드로우 범위 | **✅ 확정(사용자 결정, 2026-11-17): 진행 중 드로우만** (closeAt 이 지나지 않은 드로우) | 전체 누적으로 가면 `DropWindowRepository` 필터만 제거 |
| 동점 처리 | **✅ 확정(사용자 결정, 2026-11-17): member 사전순** (ZSet 기본 동작, 동점이어도 순번은 1, 2, …) | 3.5 참고 |
| 진행 중 필터 탐색 한도 | 배치 50, 최대 500 (구현 결정) | 아래 lazy 제거로 종료 드로우가 쌓이지 않는다. 그래도 closeAt 미상 드로우가 500개 넘게 상위를 차지하면 결과가 size 보다 적을 수 있음 |
| 종료 드로우 정리 | **확정(기본안), 2026-11-17: 조회 중 lazy ZREM** (closeAt 을 알고 지난 드로우) | 읽기 경로의 쓰기, 멱등. 종료 드로우는 인기 보드에서 빠진다. 대안: 마감 시각에 지우는 스케줄러(ranking 에 스케줄러가 새로 생김) |
| 메시지 필드 오류 | WARN 후 스킵, 재시도 없음 (확정, layered-architecture 4.5) | 저장소를 호출하지 않는다 |
| 표시 정보 없음 | `user-{id}`, `drop-{id}` (설계 ranking.md 와 통일) | 응답 `title`·`nickname` 은 null 이 아니다 |
| 리스너 실패 처리 | 3회 재시도 후 로그·스킵 (확정, 2026-11-16 — DLT 는 저장 컨슈머만) | 누락이 문제되면 랭킹 전용 DLT 도입. `entry.created.DLT` 는 entry-service 저장 컨슈머가 쓰므로 **이름을 분리**해야 한다 (예: `entry.created.ranking.DLT`, `DeadLetterPublishingRecoverer` 의 destination resolver 지정) |

### 설계 수정 필요 (아래 4건과 v1.2 결정은 착수 전 커밋 0 에서 반영. 커밋 6 은 진행 현황 갱신과 남은 문서)
- **[architecture.md](../../design/02-architecture/architecture.md) 5장 Redis 키 표**: `rank:drop:closeAt` (Hash, 쓰기 소유자 ranking) 추가. → 설계 반영 완료 (2026-11-16)
- **[ranking.md](../../design/05-features/ranking.md) 3장**: dedupe Set TTL 정책(closeAt + 7일, 최소 1일, closeAt 미상 시 30일)과 Lua KEYS 에 `rank:drop:closeAt` 포함, 집계 Lua 에서 주간 키 TTL 설정을 명시. → 설계 반영 완료 (2026-11-16)
- **[class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.5**: `DropWindow`, `DropWindowRepository` 추가, `RankingAggregateService` 에 `registerDrop`/`registerUser` 추가. → 설계 반영 완료 (2026-11-16)
- **[architecture.md](../../design/02-architecture/architecture.md) 6장 인증 제외 경로**: "랭킹 조회"를 `/api/rankings/**` 전체로 해석하면 `/api/rankings/users/me` 에 `X-User-Id` 가 주입되지 않는다. 제외 경로를 `GET /api/rankings/drops`, `/api/rankings/users/weekly`, `/api/rankings/users/all` 로 한정하도록 명시 필요 (1단계 Gateway 설정과 함께 확인). → 설계 반영 완료 (2026-11-16)

### 리스크
- **Redis 유실 시 랭킹 소실**: 진실의 원천이 Redis 뿐. 3.6 절차로 30일 이내 응모만 복구된다. 그 이상은 entry_db 기반 재집계 배치가 필요 (범위 밖).
- **표시 정보 복구 한계**: `user.registered` 보존 기간(기본 7일) 이후 Redis 가 유실되면 닉네임 복구 불가. 필요 시 `user.registered` 보존 기간 연장 또는 compacted 토픽으로 전환.
- **dedupe 만료 후 재전달**: closeAt + 7일이 지난 뒤 같은 이벤트가 재전달되면 이중 집계. 정상 운영에서는 컨슈머 지연이 7일을 넘지 않으므로 허용, 재집계는 3.6 처럼 키 삭제와 함께 수행.
- **Redis Cluster 전환 시 Lua 실패**: 키 5개가 서로 다른 슬롯. 클러스터로 가면 해시 태그(`{rank}`)로 같은 슬롯에 묶어야 한다.
- **weekly TTL 기준**: 그 주의 첫 기록 시점부터 35일이라 주차별 보관 기간이 며칠씩 다를 수 있다 (허용).
- **종료 드로우 누적으로 진행 중 드로우가 탐색 한도 밖으로 밀림**: lazy 제거 전에는 종료 드로우가 `rank:drop:popular` 에 계속 쌓여 언젠가 상위 500 을 채운다. → 조회 중 lazy ZREM 으로 대응 (3.2). 남는 한계: (1) 한 번에 지우는 수는 SCAN_LIMIT 까지, (2) 조회가 없으면 지워지지 않는다(메모리만 차지, 결과에는 영향 없음), (3) 마감 직후 늦게 온 응모가 다시 넣으면 다음 조회에서 지운다.
- **Redis 장애**: 명령 타임아웃 1초(E-03). 조회는 503, 리스너는 3회 재시도 후 스킵 → 그 사이 응모는 랭킹에서 누락된다. 복구는 3.6 재집계.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 / 항목 | 테스트 ID |
|-----------------------|-----------|
| [ ] 단위(`WeekKey`): 일요일 23:59:59 KST → 그 주 | T7-01 |
| [ ] 단위(`WeekKey`): 월요일 00:00:00 KST → 다음 주 | T7-02 |
| [ ] 단위(`WeekKey`): UTC 기준과 다른 경우 | T7-03 (+ 연도 경계 T7-04) |
| [ ] 통합: 같은 이벤트 3번 → 점수 1 | T7-13, T7-21 |
| [ ] 통합: Top N 조회 시 닉네임·드로우 제목 포함 | T7-18, T7-20 |
| [ ] 인기 드로우 진행 중 필터 동작 (탐색 한도 경계, 종료 드로우 lazy 제거 포함) | T7-09, T7-10, T7-29, T7-30 |
| [ ] 주간 키·dedupe TTL 정책 동작 | T7-15, T7-16 |
| [ ] API 4개 및 에러 매핑 (헤더 누락 400, Redis 장애 503) | T7-19, T7-27 |
| [ ] 이벤트 도착 순서 무관 | T7-22 |
| [ ] 리스너 실패 정책: 장애 3회 재시도 후 스킵, 역직렬화 실패·필드 오류는 재시도 없이 스킵 | T7-25, T7-26, T7-28 |
| [ ] 재집계 운영 절차 검증 | T7-24 |
| [ ] 공통: `./gradlew build` 통과 (ArchUnit 포함) | T7-23 |
| [x] 공통: 미결정 2건(인기 범위·동점) 확정 및 설계 문서 반영 | 커밋 0 |
| [x] 공통: "설계 수정 필요" 4건 반영 | 커밋 0 (설계 반영 완료 2026-11-16) |
