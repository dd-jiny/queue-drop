# 7단계 — 랭킹 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |

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
    │   │   │   │   ├── InvalidWeekKeyException.java                [신규]
    │   │   │   │   ├── RankingBoard.java                           [신규]
    │   │   │   │   ├── RankEntry.java                              [신규]
    │   │   │   │   └── DropWindow.java                             [신규] (구현 결정)
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
    │   │   │       ├── RankingErrorCode.java                       [신규]
    │   │   │       └── RankingException.java                       [신규]
    │   │   ├── infrastructure/
    │   │   │   ├── redis/
    │   │   │   │   ├── RankingKeys.java                            [신규]
    │   │   │   │   ├── RedisRankingRepository.java                 [신규]
    │   │   │   │   ├── RedisDisplayNameRepository.java             [신규]
    │   │   │   │   └── RedisDropWindowRepository.java              [신규] (구현 결정)
    │   │   │   └── config/
    │   │   │       ├── RankingProperties.java                      [신규]
    │   │   │       ├── RankingRedisConfig.java                     [신규] (Lua 스크립트 Bean)
    │   │   │       └── RankingKafkaConfig.java                     [신규] (에러 핸들러)
    │   │   └── presentation/
    │   │       ├── api/
    │   │       │   ├── RankingController.java                      [신규]
    │   │       │   ├── RankingExceptionHandler.java                [신규] (0단계 기본 틀 확장)
    │   │       │   └── response/
    │   │       │       ├── DropRankingResponse.java                [신규]
    │   │       │       ├── UserRankingResponse.java                [신규]
    │   │       │       └── MyRankResponse.java                     [신규]
    │   │       └── listener/
    │   │           ├── EntryCreatedListener.java                   [신규]
    │   │           ├── DropCreatedListener.java                    [신규]
    │   │           └── UserRegisteredListener.java                 [신규]
    │   └── resources/
    │       ├── application.yml                                     [수정] (ranking.* 프로퍼티)
    │       └── scripts/rank.lua                                    [신규]
    └── test/java/com/queuedrop/ranking/
        ├── ArchitectureTest.java                                   (0단계 작성분 유지)
        ├── domain/model/WeekKeyTest.java                           [신규]
        ├── domain/model/DropWindowTest.java                        [신규]
        ├── application/fake/InMemoryRankingRepository.java         [신규]
        ├── application/fake/InMemoryDisplayNameRepository.java     [신규]
        ├── application/fake/InMemoryDropWindowRepository.java      [신규]
        ├── application/service/RankingAggregateServiceTest.java    [신규]
        ├── application/service/RankingQueryServiceTest.java        [신규]
        ├── infrastructure/redis/RedisRankingRepositoryTest.java    [신규] (Testcontainers Redis)
        ├── infrastructure/redis/RedisDisplayNameRepositoryTest.java[신규]
        ├── presentation/api/RankingControllerTest.java             [신규] (@WebMvcTest)
        └── presentation/listener/RankingListenerIntegrationTest.java [신규] (Testcontainers Kafka + Redis)
```

---

## 3. 구현 상세

### 3.0 Redis 키 (설계 [architecture.md](../../design/02-architecture/architecture.md) 5장 + 추가분)

| 키 | 타입 | TTL | 설명 |
|----|------|-----|------|
| `rank:drop:popular` | ZSet | 없음 | member=dropId, score=응모 수 |
| `rank:user:weekly:{yyyy-Www}` | ZSet | **35일** (첫 기록 시 설정) | member=userId, score=주간 응모 수 |
| `rank:user:all` | ZSet | 없음 | member=userId, score=누적 응모 수 |
| `rank:dedupe:{dropId}` | Set | **closeAt + 7일** (첫 기록 시 설정) | 이미 집계한 userId |
| `rank:drop:info` | Hash | 없음 | dropId → title |
| `rank:user:info` | Hash | 없음 | userId → nickname |
| `rank:drop:closeAt` | Hash | 없음 | dropId → closeAt(epoch ms) **(구현 결정 — 설계 문서에 없는 키, 6장 "설계 수정 필요")** |

> `rank:drop:closeAt` 이 필요한 이유: (1) 인기 드로우를 "진행 중 드로우만" 보여주는 필터 (기본안 — 7단계에서 확정), (2) dedupe Set 의 만료 시각 계산.
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

```java
public class InvalidWeekKeyException extends RuntimeException {
    public InvalidWeekKeyException(String value) { super("유효하지 않은 주차: " + value); }
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
}
```
> 응모는 오픈 이후에만 들어오므로, 랭킹 ZSet 에 존재하는 드로우는 이미 오픈된 드로우다. 따라서 "진행 중" 판정은 `closeAt` 만 보면 된다. **(기본안 — 7단계에서 확정)**

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

### 3.2 application

#### DTO (application/dto)
```java
public record RankingRecordCommand(long dropId, long userId, Instant appliedAt) {}
public record DropInfoCommand(long dropId, String title, Instant closeAt) {}
public record UserInfoCommand(long userId, String nickname) {}

public record DropRankResult(long rank, long dropId, String title, long entryCount) {}
public record UserRankResult(long rank, long userId, String nickname, long score) {}
public record WeeklyRankingResult(String week, List<UserRankResult> items) {}
public record MyRankResult(String week, Long weeklyRank, long weeklyScore, Long allRank, long allScore) {}
```
> `title`, `nickname` 은 표시 정보 이벤트가 아직 안 왔으면 `null`. `weeklyRank`/`allRank` 는 랭킹에 없으면 `null`.

#### 에러 코드 (application/exception)
```java
public enum RankingErrorCode {
    INVALID_SIZE,     // size 범위(1~100) 위반 → 400
    INVALID_WEEK,     // week 형식 또는 존재하지 않는 주차 → 400
    UNAUTHENTICATED   // /me 호출 시 X-User-Id 없음 → 401
}

public class RankingException extends RuntimeException {
    private final RankingErrorCode code;
    public RankingException(RankingErrorCode code) { super(code.name()); this.code = code; }
    public RankingErrorCode code() { return code; }
}
```

#### `RankingAggregateService` — 집계·표시 정보 저장 (쓰기 경로)
```java
@Service
@RequiredArgsConstructor
public class RankingAggregateService {

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;

    /** @return 이번 호출로 집계됐으면 true, 중복이면 false */
    public boolean recordEntry(RankingRecordCommand command) {
        WeekKey week = WeekKey.of(command.appliedAt());      // 처리 시각이 아니라 응모 시각
        return rankingRepository.incrementOnce(command.dropId(), command.userId(), week);
    }

    public void registerDrop(DropInfoCommand command) {
        displayNameRepository.saveDropTitle(command.dropId(), command.title());
        dropWindowRepository.save(command.dropId(), command.closeAt());
    }

    public void registerUser(UserInfoCommand command) {
        displayNameRepository.saveNickname(command.userId(), command.nickname());
    }
}
```
> 설계(class-diagram 2.5)에는 `record(RankingRecordCommand)` 만 있으나, 표시 정보 저장도 쓰기 경로이므로 같은 서비스에 `registerDrop`/`registerUser` 를 둔다. **(구현 결정)**
> Redis 만 쓰므로 `@Transactional` 은 붙이지 않는다.

#### `RankingQueryService` — 조회 (읽기 경로)
```java
@Service
@RequiredArgsConstructor
public class RankingQueryService {

    private static final int MAX_SIZE = 100;
    private static final int SCAN_BATCH = 50;          // (구현 결정)
    private static final int SCAN_LIMIT = 500;         // 진행 중 필터 시 최대 탐색 수 (구현 결정)

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;
    private final Clock clock;                          // java.time.Clock Bean (infrastructure/config 에서 등록)

    /** 진행 중 드로우만 응모 수 순으로 (기본안 — 7단계에서 확정) */
    public List<DropRankResult> topDrops(int size) {
        validateSize(size);
        Instant now = clock.instant();
        List<RankEntry> picked = new ArrayList<>();
        for (long offset = 0; picked.size() < size && offset < SCAN_LIMIT; offset += SCAN_BATCH) {
            List<RankEntry> batch = rankingRepository.top(RankingBoard.DROP_POPULAR, null, offset, SCAN_BATCH);
            if (batch.isEmpty()) break;
            Map<Long, DropWindow> windows = dropWindowRepository.findAll(ids(batch));
            batch.stream()
                 .filter(e -> windows.get(e.memberId()).isInProgress(now))
                 .limit(size - picked.size())
                 .forEach(picked::add);
        }
        Map<Long, String> titles = displayNameRepository.findDropTitles(ids(picked));
        // 필터 후 순번을 1부터 다시 매긴다 (구현 결정)
        return IntStream.range(0, picked.size())
                .mapToObj(i -> new DropRankResult(i + 1, picked.get(i).memberId(),
                        titles.get(picked.get(i).memberId()), picked.get(i).score()))
                .toList();
    }

    /** week 가 null 이면 이번 주 */
    public WeeklyRankingResult weeklyUsers(String week, int size) {
        validateSize(size);
        WeekKey key = (week == null) ? WeekKey.of(clock.instant()) : parseWeek(week);
        List<RankEntry> entries = rankingRepository.top(RankingBoard.USER_WEEKLY, key, 0, size);
        return new WeeklyRankingResult(key.value(), withNicknames(entries));
    }

    public List<UserRankResult> allUsers(int size) {
        validateSize(size);
        return withNicknames(rankingRepository.top(RankingBoard.USER_ALL, null, 0, size));
    }

    public MyRankResult myRank(long userId) {
        WeekKey thisWeek = WeekKey.of(clock.instant());
        Optional<RankEntry> weekly = rankingRepository.rankOf(RankingBoard.USER_WEEKLY, thisWeek, userId);
        Optional<RankEntry> all = rankingRepository.rankOf(RankingBoard.USER_ALL, null, userId);
        return new MyRankResult(thisWeek.value(),
                weekly.map(RankEntry::rank).orElse(null), weekly.map(RankEntry::score).orElse(0L),
                all.map(RankEntry::rank).orElse(null), all.map(RankEntry::score).orElse(0L));
    }

    private WeekKey parseWeek(String week) {
        try { return WeekKey.parse(week); }
        catch (InvalidWeekKeyException e) { throw new RankingException(RankingErrorCode.INVALID_WEEK); }
    }

    private void validateSize(int size) {
        if (size < 1 || size > MAX_SIZE) throw new RankingException(RankingErrorCode.INVALID_SIZE);
    }
    // ids(), withNicknames() ...
}
```
- public 메서드의 파라미터·반환은 원시값·String·Result 뿐이다 (ArchUnit R6). `RankEntry`, `WeekKey` 는 내부에서만 쓴다.
- `Clock` 주입으로 테스트에서 시각을 고정한다.

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
local dropId = ARGV[1]
local userId = ARGV[2]

-- 1) 멱등 처리: 이미 집계한 응모면 아무것도 하지 않는다
if redis.call('SADD', KEYS[1], userId) == 0 then
    return 0
end

-- 2) dedupe Set 만료 시각: closeAt + 유예. 단, 최소 TTL 보장
--    (재처리 시 closeAt 이 이미 지났더라도 키가 즉시 삭제되지 않도록)
if redis.call('PTTL', KEYS[1]) == -1 then
    local now = tonumber(ARGV[7])
    local closeAt = redis.call('HGET', KEYS[5], dropId)
    local expireAt
    if closeAt then
        expireAt = tonumber(closeAt) + tonumber(ARGV[4])
    else
        expireAt = now + tonumber(ARGV[5])
    end
    local minExpireAt = now + tonumber(ARGV[6])
    if expireAt < minExpireAt then expireAt = minExpireAt end
    redis.call('PEXPIREAT', KEYS[1], expireAt)
end

-- 3) 3개 보드 증가
redis.call('ZINCRBY', KEYS[2], 1, dropId)
redis.call('ZINCRBY', KEYS[3], 1, userId)
if redis.call('TTL', KEYS[3]) == -1 then
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[3]))
end
redis.call('ZINCRBY', KEYS[4], 1, userId)

return 1
```
- 모든 명령이 O(1)/O(log N) 이라 스크립트가 Redis 를 오래 붙잡지 않는다.
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
ranking:
  weekly-ttl: ${RANKING_WEEKLY_TTL:35d}
  dedupe-grace: ${RANKING_DEDUPE_GRACE:7d}
  dedupe-fallback-ttl: ${RANKING_DEDUPE_FALLBACK_TTL:30d}
  dedupe-min-ttl: ${RANKING_DEDUPE_MIN_TTL:1d}
```

#### `RankingRedisConfig`
```java
@Configuration
@EnableConfigurationProperties(RankingProperties.class)
class RankingRedisConfig {

    @Bean
    RedisScript<Long> rankScript() {
        return RedisScript.of(new ClassPathResource("scripts/rank.lua"), Long.class);
    }

    @Bean
    Clock clock() { return Clock.systemUTC(); }   // 주차 계산은 WeekKey 가 Asia/Seoul 로 변환
}
```

#### `RedisRankingRepository` — `RankingRepository` 구현
```java
@Repository
@RequiredArgsConstructor
class RedisRankingRepository implements RankingRepository {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> rankScript;
    private final RankingProperties props;
    private final Clock clock;

    @Override
    public boolean incrementOnce(long dropId, long userId, WeekKey week) {
        List<String> keys = List.of(
                RankingKeys.dedupe(dropId), RankingKeys.DROP_POPULAR, RankingKeys.weekly(week),
                RankingKeys.USER_ALL, RankingKeys.DROP_CLOSE_AT);
        Long result = redis.execute(rankScript, keys,
                String.valueOf(dropId), String.valueOf(userId),
                String.valueOf(props.weeklyTtl().toSeconds()),
                String.valueOf(props.dedupeGrace().toMillis()),
                String.valueOf(props.dedupeFallbackTtl().toMillis()),
                String.valueOf(props.dedupeMinTtl().toMillis()),
                String.valueOf(clock.millis()));
        return Long.valueOf(1L).equals(result);
    }

    @Override
    public List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count) {
        Set<ZSetOperations.TypedTuple<String>> tuples = redis.opsForZSet()
                .reverseRangeWithScores(RankingKeys.board(board, week), offset, offset + count - 1);
        // tuples 는 점수 내림차순. rank = offset + index + 1
        // ...
    }

    @Override
    public Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId) {
        String key = RankingKeys.board(board, week);
        Long rank = redis.opsForZSet().reverseRank(key, String.valueOf(memberId));
        if (rank == null) return Optional.empty();
        Double score = redis.opsForZSet().score(key, String.valueOf(memberId));
        return Optional.of(new RankEntry(rank + 1, memberId, score.longValue()));
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

#### `RankingKafkaConfig` — 리스너 에러 처리 (구현 결정)
```java
@Configuration
class RankingKafkaConfig {

    /** 1초 간격 3회 재시도 후 로그 남기고 건너뜀. 랭킹은 응모의 진실의 원천이 아니므로 DLT 없이 시작. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, ex) -> log.error("ranking 이벤트 처리 실패, 건너뜀: topic={}, key={}",
                        record.topic(), record.key(), ex),
                new FixedBackOff(1000L, 3L));
        return handler;
    }
}
```
> 역직렬화 설정(JSON → common 이벤트 record)은 0단계 공통 설정을 그대로 쓴다.

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
- `drop.created` 보다 `entry.created` 가 먼저 와도 문제없다: 제목은 조회 시 `null`, dedupe TTL 은 기본 TTL(30일)이 적용된다.

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

    @GetMapping("/users/me")
    MyRankResponse me(@RequestHeader(value = "X-User-Id", required = false) Long userId) {
        if (userId == null) throw new RankingException(RankingErrorCode.UNAUTHENTICATED);
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

#### `RankingExceptionHandler`
| 에러 코드 | HTTP |
|-----------|------|
| `INVALID_SIZE`, `INVALID_WEEK` | 400 |
| `UNAUTHENTICATED` | 401 |

응답 바디는 0단계 공통 에러 형식(`{code, message}`)을 따른다.

### 3.5 동점 처리 (기본안 — 7단계에서 확정)
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
| T7-11 | application 단위 (Fake) | `RankingQueryServiceTest` | size 0, 101 → `INVALID_SIZE` · week `"abc"` → `INVALID_WEEK` · week null → 현재 시각(Clock 고정)의 주차 | REQ-RANK-02 |
| T7-12 | application 단위 (Fake) | `RankingQueryServiceTest` | Given 랭킹에 없는 사용자 / When `myRank` / Then rank null, score 0 | REQ-RANK-02 |
| T7-13 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | Given 같은 (dropId, userId) / When `incrementOnce` 3번 / Then 첫 호출만 true, 3개 보드 점수 모두 1 | REQ-RANK-03 |
| T7-14 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | Given 같은 드로우에 사용자 2명 / When 각 1회 / Then popular=2, 각 사용자 weekly=1, all=1 | REQ-RANK-01, 02 |
| T7-15 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | 첫 기록 후 weekly 키 TTL ≈ 35일, 두 번째 기록 후에도 TTL 이 연장되지 않음 | REQ-RANK-02 |
| T7-16 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | dedupe TTL: closeAt 있음 → closeAt+7일 / closeAt 없음 → now+30일 / closeAt 이 10일 전 → now+1일 (최소 TTL) | REQ-RANK-03 |
| T7-17 | infra 통합 (Redis) | `RedisRankingRepositoryTest` | 동점 2명 → `ZREVRANGE` 사전 역순, 순번 1·2 | (동점 정책) |
| T7-18 | infra 통합 (Redis) | `RedisDisplayNameRepositoryTest` | 제목·닉네임 저장 후 일괄 조회, 없는 id 는 맵에서 제외, 같은 값 재저장 멱등 | REQ-RANK-01, 02 |
| T7-19 | presentation 슬라이스 | `RankingControllerTest` | `/users/me` 헤더 없음 → 401 · `?size=0` → 400 · `?week=2026-W99` → 400 · 정상 응답 JSON 필드 확인 | REQ-RANK-01, 02 |
| T7-20 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | Given `user.registered`, `drop.created`, `entry.created` 발행 / When 소비 완료(await) / Then `/api/rankings/drops` 에 제목, `/users/all` 에 닉네임 포함 | REQ-RANK-01~03 |
| T7-21 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | 같은 `entry.created` 메시지 3번 발행 → 점수 1 | REQ-RANK-03 |
| T7-22 | 통합 (Kafka + Redis) | `RankingListenerIntegrationTest` | `entry.created` 가 `drop.created` 보다 먼저 도착 → 집계 정상, 제목 null, 이후 `drop.created` 도착 시 제목 채워짐 | REQ-RANK-03 |
| T7-23 | 아키텍처 | `ArchitectureTest` | R1~R7 통과 (특히 presentation → domain 의존 없음, application 시그니처에 `WeekKey`/`RankEntry` 없음) | layered |
| T7-24 | 운영 절차 (수동) | 3.6 절차 | 응모 50건 집계 → 집계 키 삭제·오프셋 리셋·재시작 → 점수 동일하게 복구 | NFR-03 |

> 통합 테스트의 비동기 검증은 Awaitility(`await().atMost(10s)`)를 쓴다 (0단계 testFixtures 에 포함되지 않았다면 이 단계에서 추가 — 구현 결정).

---

## 5. 커밋 계획

| 순서 | 커밋 메시지 | 포함 |
|------|-------------|------|
| 1 | `feat(ranking): 주차 값 객체와 랭킹 도메인 모델 추가`<br>- WeekKey (Asia/Seoul ISO 주차, 파싱·검증)<br>- RankingBoard, RankEntry, DropWindow<br>- 저장소 포트 3개 | domain 전체, T7-01~07 |
| 2 | `feat(ranking): 랭킹 집계·조회 유스케이스 추가`<br>- RankingAggregateService, RankingQueryService<br>- Command/Result DTO, 에러 코드<br>- Fake 저장소 기반 단위 테스트 | application 전체, T7-08~12 |
| 3 | `feat(ranking): Redis 랭킹 저장소와 집계 Lua 스크립트 구현`<br>- rank.lua (dedupe + ZINCRBY 3개, TTL 처리)<br>- RedisRankingRepository, RedisDisplayNameRepository, RedisDropWindowRepository<br>- RankingProperties, Lua 스크립트 Bean | infra redis·config, `application.yml`, T7-13~18 |
| 4 | `feat(ranking): 랭킹 이벤트 리스너 추가`<br>- entry.created / drop.created / user.registered 소비<br>- 재시도 후 건너뛰는 에러 핸들러 | listener, `RankingKafkaConfig`, T7-20~22 |
| 5 | `feat(ranking): 랭킹 조회 API 추가`<br>- 인기 드로우, 주간·누적 유저, 내 순위<br>- 에러 코드 → HTTP 매핑 | controller, response, handler, T7-19 |
| 6 | `docs: 랭킹 정책 확정 및 설계 문서 반영`<br>- 인기 드로우 범위·동점 처리 결정<br>- rank:drop:closeAt 키 추가 반영 | 설계 문서 수정, 플랜 진행 현황 |

---

## 6. 결정 필요 · 리스크

### 결정 필요
| 항목 | 기본안 | 비고 |
|------|--------|------|
| 인기 드로우 범위 | **진행 중 드로우만** (closeAt 이 지나지 않은 드로우) — 기본안 — 7단계에서 확정 | 전체 누적으로 가면 `DropWindowRepository` 필터만 제거 |
| 동점 처리 | **member 사전순** (ZSet 기본 동작) — 기본안 — 7단계에서 확정 | 3.5 참고 |
| 진행 중 필터 탐색 한도 | 배치 50, 최대 500 (구현 결정) | 종료 드로우가 500개 넘게 상위를 차지하면 결과가 size 보다 적을 수 있음 |
| 리스너 실패 처리 | 3회 재시도 후 로그·스킵 (구현 결정) | 누락이 문제되면 DLT(`entry.created.DLT`) 도입 |

### 설계 수정 필요 (설계 문서는 이 단계 커밋 6에서 수정)
- **[architecture.md](../../design/02-architecture/architecture.md) 5장 Redis 키 표**: `rank:drop:closeAt` (Hash, 쓰기 소유자 ranking) 추가.
- **[ranking.md](../../design/05-features/ranking.md) 3장**: dedupe Set TTL 정책(closeAt + 7일, 최소 1일, closeAt 미상 시 30일)과 Lua KEYS 에 `rank:drop:closeAt` 포함, 집계 Lua 에서 주간 키 TTL 설정을 명시.
- **[class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.5**: `DropWindow`, `DropWindowRepository` 추가, `RankingAggregateService` 에 `registerDrop`/`registerUser` 추가.
- **[architecture.md](../../design/02-architecture/architecture.md) 6장 인증 제외 경로**: "랭킹 조회"를 `/api/rankings/**` 전체로 해석하면 `/api/rankings/users/me` 에 `X-User-Id` 가 주입되지 않는다. 제외 경로를 `GET /api/rankings/drops`, `/api/rankings/users/weekly`, `/api/rankings/users/all` 로 한정하도록 명시 필요 (1단계 Gateway 설정과 함께 확인).

### 리스크
- **Redis 유실 시 랭킹 소실**: 진실의 원천이 Redis 뿐. 3.6 절차로 30일 이내 응모만 복구된다. 그 이상은 entry_db 기반 재집계 배치가 필요 (범위 밖).
- **표시 정보 복구 한계**: `user.registered` 보존 기간(기본 7일) 이후 Redis 가 유실되면 닉네임 복구 불가. 필요 시 `user.registered` 보존 기간 연장 또는 compacted 토픽으로 전환.
- **dedupe 만료 후 재전달**: closeAt + 7일이 지난 뒤 같은 이벤트가 재전달되면 이중 집계. 정상 운영에서는 컨슈머 지연이 7일을 넘지 않으므로 허용, 재집계는 3.6 처럼 키 삭제와 함께 수행.
- **Redis Cluster 전환 시 Lua 실패**: 키 5개가 서로 다른 슬롯. 클러스터로 가면 해시 태그(`{rank}`)로 같은 슬롯에 묶어야 한다.
- **weekly TTL 기준**: 그 주의 첫 기록 시점부터 35일이라 주차별 보관 기간이 며칠씩 다를 수 있다 (허용).

---

## 7. 완료 체크리스트

| 플랜 완료 조건 / 항목 | 테스트 ID |
|-----------------------|-----------|
| [ ] 단위(`WeekKey`): 일요일 23:59:59 KST → 그 주 | T7-01 |
| [ ] 단위(`WeekKey`): 월요일 00:00:00 KST → 다음 주 | T7-02 |
| [ ] 단위(`WeekKey`): UTC 기준과 다른 경우 | T7-03 (+ 연도 경계 T7-04) |
| [ ] 통합: 같은 이벤트 3번 → 점수 1 | T7-13, T7-21 |
| [ ] 통합: Top N 조회 시 닉네임·드로우 제목 포함 | T7-18, T7-20 |
| [ ] 인기 드로우 진행 중 필터 동작 | T7-09, T7-10 |
| [ ] 주간 키·dedupe TTL 정책 동작 | T7-15, T7-16 |
| [ ] API 4개 및 에러 매핑 | T7-19 |
| [ ] 이벤트 도착 순서 무관 | T7-22 |
| [ ] 재집계 운영 절차 검증 | T7-24 |
| [ ] 공통: `./gradlew build` 통과 (ArchUnit 포함) | T7-23 |
| [ ] 공통: 미결정 2건(인기 범위·동점) 확정 및 설계 문서 반영 | 커밋 6 |
| [ ] 공통: "설계 수정 필요" 4건 반영 | 커밋 6 |
