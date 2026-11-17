# 5단계 — 추첨 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 설계 반영 완료 표시 |
| v1.2 | 2026-11-17 | 착수 전 정리. D5-1 확정(사용자 결정: 경고 후 관리자 수동 추첨 API 포함, 3.2·3.4). E-42 확정(기본안: 추첨 완료 후 응모 키 6개 EXPIRE 1일, 3.2·3.3). E-41 개선안 2(Redis∖DB 대조)는 만들지 않음 — 수동 추첨이 DB 기준이라 Redis 에만 남은 응모는 빠진다(6장). E-30 반영(3.5 를 2단계 구현에 맞춤). 에러 코드를 0단계 계약(`ApplicationException` + `{SERVICE}_{REASON}`)에 맞춤 |
| v1.3 | 2026-11-17 | 커밋 순서 변경 #2 영속성 → #3 Redis·Kafka 어댑터 → #4 유스케이스 — 포트 확장을 구현체·Fake 와 같은 커밋에 두어 커밋마다 컴파일·전체 테스트가 통과하게. 저장소 포트 id 인자를 원시 `long` 으로 통일(4단계 포트와 동일). T5-33 추가 |
| v1.4 | 2026-11-17 | 마이그레이션 번호 V3 → V2 (entry 는 V1 만 있음). T5-34(DrawResult 영속) 추가. 벌크 UPDATE clearAutomatically |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 응모 마감 시각에 자동으로 추첨하고 결과를 조회할 수 있게 한다. 드로우 흐름(등록 → 대기열 → 응모 → 추첨 → 결과) 전체를 완성한다 |
| 요구사항 | REQ-ENTRY-05, REQ-ENTRY-06, NFR-03, NFR-04 |
| 브랜치 | `feat/phase-5-draw` |
| 대상 서비스 | entry-service (추첨), product-service (`draw.completed` 수신) |

**선행 조건**
- 0단계: `common` 이벤트(`DrawCompleted`)·`Topics`, ArchUnit testFixtures, Testcontainers 지원, Flyway, 공통 에러 응답
- 2단계: product-service `Drop`(`markDrawn()`), `DropRepository`, `drops.drawn` 컬럼
- 4단계: entry-service `Entry`, `EntryStatus`, `DropSnapshot`, `SnapshotStatus`, `EntryRepository`, `DropSnapshotRepository`, `EntryGate`, `apply.lua`(`meta.state` 검사 포함), `drop_snapshots`·`entries` 테이블

**참고 설계 문서**
- [draw.md](../../design/05-features/draw.md) — 추첨 설계 본문
- [concurrency-consistency.md](../../design/04-non-functional/concurrency-consistency.md) D5 ~ D7
- [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.2, 2.2
- [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 2.2 ④, 5.4
- [erd.md](../../design/06-erd/erd.md) entry_db
- [layered-architecture.md](../../design/02-architecture/layered-architecture.md)

---

## 2. 산출물 파일 목록

```
entry-service/src/main/java/com/queuedrop/entry/
├── domain
│   ├── model
│   │   ├── DrawResult.java                          [신규]
│   │   └── DropSnapshot.java                        [수정] isDue(now), isClosingTooLong(now, threshold), isClosing(), isDrawn()
│   ├── service
│   │   └── Drawer.java                              [신규]
│   └── repository
│       ├── DrawResultRepository.java                [신규]
│       ├── DropSnapshotRepository.java              [수정] markClosingIfOpen, findDueOpen, findClosing, markDrawnIfClosing
│       └── EntryRepository.java                     [수정] countByDropId, findUserIdsOrderBySeq, markResults
├── application
│   ├── service
│   │   ├── DrawService.java                         [신규] 마감·추첨 조율 (트랜잭션 밖)
│   │   ├── DrawTransaction.java                     [신규] 추첨 단일 트랜잭션 (구현 결정)
│   │   └── DrawQueryService.java                    [신규]
│   ├── dto
│   │   ├── DrawSummaryResult.java                   [신규]
│   │   ├── ManualDrawCommand.java                   [신규] 관리자 수동 추첨 (D5-1)
│   │   ├── Requester.java                           [신규] userId + role (product 와 같은 형태)
│   │   ├── DrawCompletedPayload.java                [신규]
│   │   └── DrawOutcome.java                         [신규] DrawTransaction 결과
│   ├── exception
│   │   └── DrawErrorCode.java                       [신규]
│   └── port
│       ├── ApplicantCounter.java                    [신규]
│       ├── DrawEventPublisher.java                  [신규]
│       ├── SeedGenerator.java                       [신규] (구현 결정)
│       └── EntryGate.java                           [수정] close(dropId), expireAfterDraw(dropId, ttl) 추가
├── infrastructure
│   ├── persistence
│   │   ├── DrawResultJpaEntity.java                 [신규]
│   │   ├── DrawResultJpaRepository.java             [신규]
│   │   ├── DrawResultRepositoryImpl.java            [신규]
│   │   ├── DrawResultMapper.java                    [신규]
│   │   ├── DropSnapshotJpaRepository.java           [수정] 조건부 UPDATE 쿼리
│   │   ├── DropSnapshotRepositoryImpl.java          [수정]
│   │   ├── EntryJpaRepository.java                  [수정] 벌크 UPDATE, 정렬 조회
│   │   └── EntryRepositoryImpl.java                 [수정]
│   ├── redis
│   │   ├── RedisEntryGate.java                      [수정] close(), expireAfterDraw()
│   │   └── RedisApplicantCounter.java               [신규]
│   ├── messaging
│   │   └── KafkaDrawEventPublisher.java             [신규]
│   ├── random
│   │   └── SecureRandomSeedGenerator.java           [신규]
│   └── config
│       └── SchedulingConfig.java                    [신규] @EnableScheduling, Clock 빈
└── presentation
    ├── scheduler
    │   └── DrawScheduler.java                       [신규]
    └── api
        ├── DrawResultController.java                [신규]
        ├── AdminDrawController.java                 [신규] 관리자 수동 추첨 (D5-1)
        └── response/DrawSummaryResponse.java        [신규]

entry-service/src/main/resources/
├── db/migration/V2__create_draw_results.sql         [신규]
└── application.yml                                  [수정] entry.draw.* 설정

product-service/src/main/java/com/queuedrop/product/
├── application/service/DropService.java             [수정] markDrawn(MarkDrawnCommand)
├── infrastructure/config/KafkaConsumerConfig.java   [신규] product 첫 컨슈머. DefaultErrorHandler + DROP_NOT_FOUND 비재시도 (entry 4단계 설정과 같은 형태)
├── application/dto/MarkDrawnCommand.java            [신규]
└── presentation/listener/DrawCompletedListener.java [신규]

e2e-test/                                            [신규 모듈] (구현 결정, 3.4 참고)
├── build.gradle
└── src/test/java/com/queuedrop/e2e/DropFlowE2ETest.java
scripts/e2e.sh                                       [신규]
settings.gradle                                      [수정] include 'e2e-test'
```

---

## 3. 구현 상세

### 3.1 entry-service — domain

#### `Drawer` — 추첨 순수 함수 (도메인 서비스)
정렬된 응모자 목록, 당첨 수 K, 시드를 받아 당첨자 집합을 돌려준다. Spring·저장소 의존 없음.

```java
package com.queuedrop.entry.domain.service;

public final class Drawer {

    /**
     * @param orderedUserIds entry_seq 오름차순으로 정렬된 응모자 목록 (결정적 입력)
     * @param winnerCount    K (≥ 1)
     * @param seed           셔플 시드 (draw_results 에 저장되어 재현에 사용)
     * @return 당첨자 userId 집합, 크기 = min(K, 응모자 수)
     */
    public Set<Long> draw(List<Long> orderedUserIds, int winnerCount, long seed) {
        if (winnerCount < 1) {
            throw new DomainException("winnerCount must be >= 1");
        }
        List<Long> shuffled = new ArrayList<>(orderedUserIds);   // 입력 불변 보장
        Collections.shuffle(shuffled, new Random(seed));          // Fisher–Yates
        int k = Math.min(winnerCount, shuffled.size());
        return Set.copyOf(shuffled.subList(0, k));
    }
}
```
- `Collections.shuffle(List, Random)` 은 JDK 명세상 Fisher–Yates 로 구현되어 있고, **같은 입력 순서 + 같은 시드 = 같은 결과**가 보장된다.
- 입력 순서가 결과에 영향을 주므로 반드시 `entry_seq` 오름차순 목록을 넘긴다 (호출자 책임, `EntryRepository.findUserIdsOrderBySeq`).
- `java.util.Random` 은 시드 재현용이고, 시드 자체는 `SecureRandom` 으로 만든다 (3.2 `SeedGenerator`).

#### `DrawResult` — 추첨 기록
```java
package com.queuedrop.entry.domain.model;

public final class DrawResult {
    private final Long id;
    private final Long dropId;
    private final long seed;
    private final int applicantCount;
    private final int winnerCount;     // 실제 당첨 수 = min(K, 응모자 수)
    private final Instant drawnAt;
    private final boolean published;   // draw.completed 발행 여부 (구현 결정, 6장 설계 수정 필요)

    public static DrawResult of(Long dropId, long seed, int applicantCount, int winnerCount, Instant drawnAt) {
        // 검증: applicantCount ≥ 0, 0 ≤ winnerCount ≤ applicantCount
        // ...
    }

    public DrawResult markPublished() { /* published=true 인 새 인스턴스 */ }
    // getter ...
}
```

#### `DropSnapshot` (수정)
```java
public boolean isDue(Instant now) {
    return status == SnapshotStatus.OPEN && !now.isBefore(closeAt);   // closeAt <= now
}

public boolean isClosingTooLong(Instant now, Duration threshold) {
    return status == SnapshotStatus.CLOSING && now.isAfter(closeAt.plus(threshold));
}

public boolean isClosing() { return status == SnapshotStatus.CLOSING; }   // 수동 추첨 검사 (D5-1)
public boolean isDrawn()   { return status == SnapshotStatus.DRAWN; }
```
- CLOSING 진입 시각을 따로 저장하지 않고 `closeAt` 기준으로 장기화를 판단한다 (구현 결정, 컬럼 추가 불필요).

#### Repository 포트 (수정·신규)
```java
public interface DropSnapshotRepository {
    // ... 4단계 메서드 (saveIfAbsent, findById(long))

    /** status=OPEN 이고 closeAt <= now 인 드로우 id 목록 */
    List<Long> findDueOpenIds(Instant now);

    /** OPEN → CLOSING 조건부 전이. 이 호출이 전이시켰으면 true (다중 인스턴스 중 1곳만 true) */
    boolean markClosingIfOpen(long dropId, Instant now);

    List<DropSnapshot> findClosing();

    /** CLOSING → DRAWN 조건부 전이. 추첨 트랜잭션의 첫 문장 (행 잠금 획득) */
    boolean markDrawnIfClosing(long dropId);
}

public interface EntryRepository {
    // ... 4단계 메서드 (saveIfAbsent, findByDropIdAndUserId, findAllByUserId)
    long countByDropId(long dropId);
    List<Long> findUserIdsOrderBySeq(long dropId);
    /** winners → WON, 나머지 APPLIED → LOST (벌크) */
    void markResults(long dropId, Set<Long> winnerUserIds);
}

public interface DrawResultRepository {
    DrawResult save(DrawResult result);
    Optional<DrawResult> findByDropId(long dropId);
    List<DrawResult> findUnpublished();
    void markPublished(long dropId);
}
```
- id 인자는 4단계 포트와 같이 원시 `long` 으로 통일한다 (구현 결정, v1.3).
- 포트 확장은 구현체(`*RepositoryImpl`)·테스트 Fake 와 같은 커밋(#2)에 넣는다. 포트만 먼저 늘리면 그 커밋에서 컴파일이 깨진다.

---

### 3.2 entry-service — application

#### 포트
```java
package com.queuedrop.entry.application.port;

public interface ApplicantCounter {
    long count(Long dropId);                 // Redis SCARD entry:{dropId}:applicants
}

public interface DrawEventPublisher {
    void publishCompleted(DrawCompletedPayload payload);   // 실패 시 예외
}

/** 테스트에서 고정 시드를 주입하기 위한 포트 (구현 결정) */
public interface SeedGenerator {
    long next();
}

public interface EntryGate {
    // ... 4단계: tryApply, compensate, isApplicant, initialize
    void close(Long dropId);                 // HSET entry:{dropId}:meta state CLOSED
    /** 추첨 커밋 후 응모 키 6개(meta, remaining, applicants, seq, requests, persisted)에 EXPIRE. 실패 시 GateUnavailableException (E-42) */
    void expireAfterDraw(Long dropId, Duration ttl);
}
```
> `expireAfterDraw` 는 DB 가 `DRAWN` 으로 커밋된 뒤에만 부른다. 이후 판단 근거는 DB(`entries.status`)뿐이고, 늦게 도착한 `entry.created` 는 `entry-claim.lua` 가 `applicants` 에 없어서 0 → 저장하지 않는다. queue 토큰 키는 entry 소유가 아니라 건드리지 않는다.
> `SeedGenerator` 를 application 포트로 둔 이유: 시드 생성은 도메인 규칙이 아니라 "무작위 소스" 라는 외부 자원이고, 테스트에서 고정값을 넣어 결과를 검증해야 한다.

#### DTO
```java
public record DrawCompletedPayload(Long dropId, int applicantCount, int winnerCount, Instant drawnAt) {}

public record DrawSummaryResult(Long dropId, int applicantCount, int winnerCount, long seed, Instant drawnAt) {
    static DrawSummaryResult from(DrawResult r) { /* application 내부에서만 변환 */ }
}

/** DrawTransaction 실행 결과. domain 타입을 밖으로 내보내지 않기 위한 application DTO */
public record ManualDrawCommand(Requester requester, Long dropId) {}

public record Requester(Long userId, String role) {
    public boolean isAdmin() { return "ADMIN".equals(role); }
}

public record DrawOutcome(boolean drawn, DrawCompletedPayload payload) {
    static DrawOutcome skipped() { return new DrawOutcome(false, null); }
}
```

#### 에러 코드
```java
/** EntryErrorCode 와 같은 형태. 던질 때는 new ApplicationException(DrawErrorCode.X) (0단계 계약) */
public enum DrawErrorCode implements ErrorCode {
    DROP_NOT_FOUND("ENTRY_DRAW_DROP_NOT_FOUND", 404, "드로우를 찾을 수 없습니다."),
    NOT_COMPLETED("ENTRY_DRAW_NOT_COMPLETED", 409, "아직 추첨이 완료되지 않았습니다."),
    NOT_CLOSING("ENTRY_DRAW_NOT_CLOSING", 409, "마감 처리 중인 드로우만 수동 추첨할 수 있습니다."),  // D5-1
    ALREADY_DRAWN("ENTRY_DRAW_ALREADY_DRAWN", 409, "이미 추첨된 드로우입니다.");                   // D5-1
    // code/status/message 필드·접근자는 EntryErrorCode 와 동일
}
```
- 관리자 권한 없음은 공통 `CommonErrorCode.FORBIDDEN` (product `AdminGuard` 와 같은 방식).

#### `DrawService` — 마감·추첨 조율 (트랜잭션 밖)
스케줄러가 매 주기 호출한다. 트랜잭션을 직접 열지 않고, 추첨 트랜잭션은 `DrawTransaction` 에 위임한다.

```java
@Service
@RequiredArgsConstructor
public class DrawService {

    private final DropSnapshotRepository snapshots;
    private final EntryRepository entries;
    private final DrawResultRepository results;
    private final ApplicantCounter applicantCounter;
    private final EntryGate gate;
    private final DrawTransaction drawTransaction;
    private final DrawEventPublisher publisher;
    private final DrawProperties properties;     // closingWarnThreshold 등 (application 설정 record)

    /** 1) 마감 잠금: OPEN → CLOSING, Redis state=CLOSED */
    public void closeDueDrops(Instant now) {
        for (Long dropId : snapshots.findDueOpenIds(now)) {
            if (snapshots.markClosingIfOpen(dropId, now)) {   // 1곳만 true
                gate.close(dropId);
                log.info("drop closed dropId={}", dropId);
            }
        }
    }

    /** 2) CLOSING 드로우마다 건수 비교 후 추첨 */
    public void drawReadyDrops(Instant now) {
        for (DropSnapshot snapshot : snapshots.findClosing()) {
            drawIfReady(snapshot, now);
        }
    }

    private void drawIfReady(DropSnapshot snapshot, Instant now) {
        Long dropId = snapshot.getDropId();
        long redisCount = applicantCounter.count(dropId);
        long dbCount = entries.countByDropId(dropId);

        if (redisCount != dbCount) {
            if (snapshot.isClosingTooLong(now, properties.closingWarnThreshold())) {
                log.warn("draw waiting too long dropId={} redis={} db={}", dropId, redisCount, dbCount);
            }
            return;                                          // 다음 주기 재확인
        }

        DrawOutcome outcome = drawTransaction.execute(dropId, now);   // 커밋까지 완료
        if (outcome.drawn()) {
            afterCommit(outcome.payload());
        }
    }

    /** 관리자 수동 추첨 (D5-1): 건수 비교 없이 DB 응모 기준으로 추첨. Redis 에만 있는 응모는 빠진다 */
    public DrawSummaryResult drawManually(ManualDrawCommand command, Instant now) {
        AdminGuard.requireAdmin(command.requester());          // 403 FORBIDDEN
        Long dropId = command.dropId();
        DropSnapshot snapshot = snapshots.findById(dropId)
                .orElseThrow(() -> new ApplicationException(DrawErrorCode.DROP_NOT_FOUND));
        if (snapshot.isDrawn()) throw new ApplicationException(DrawErrorCode.ALREADY_DRAWN);
        if (!snapshot.isClosing()) throw new ApplicationException(DrawErrorCode.NOT_CLOSING);   // OPEN: 마감 전

        long redisCount = safeCount(dropId);                    // 감사 로그용. Redis 장애여도 수동 추첨은 진행
        long dbCount = entries.countByDropId(dropId);
        log.warn("manual draw dropId={} by userId={} redis={} db={}",
                dropId, command.requester().userId(), redisCount, dbCount);

        DrawOutcome outcome = drawTransaction.execute(dropId, now);
        if (!outcome.drawn()) throw new ApplicationException(DrawErrorCode.ALREADY_DRAWN);  // 스케줄러와 경합해 짐
        afterCommit(outcome.payload());
        return results.findByDropId(dropId).map(DrawSummaryResult::from).orElseThrow();
    }

    /** 커밋 후: 발행(실패 시 재발행 대상) → 응모 키 만료 (E-42). 둘 다 실패해도 추첨 결과는 확정 */
    private void afterCommit(DrawCompletedPayload payload) {
        publishSafely(payload);
        try {
            gate.expireAfterDraw(payload.dropId(), properties.keyTtlAfterDraw());
        } catch (GateUnavailableException e) {
            log.warn("entry keys expire failed dropId={}", payload.dropId(), e);   // 키만 남음. 정합성 영향 없음
        }
    }

    /** 3) 발행 실패분 재발행 (구현 결정) */
    public void republishUnpublished() {
        for (DrawResult r : results.findUnpublished()) {
            publishSafely(new DrawCompletedPayload(r.getDropId(), r.getApplicantCount(), r.getWinnerCount(), r.getDrawnAt()));
        }
    }

    private void publishSafely(DrawCompletedPayload payload) {
        try {
            publisher.publishCompleted(payload);
            results.markPublished(payload.dropId());
        } catch (RuntimeException e) {
            log.warn("draw.completed publish failed dropId={}", payload.dropId(), e);   // 다음 주기 재발행
        }
    }
}
```
- `DropSnapshot` 은 `DrawService` 내부(private)에서만 쓰이고 public 시그니처에 노출되지 않는다 (ArchUnit R6).
- `AdminGuard` 는 entry application 에 product 와 같은 형태로 둔다 (package-private, `Requester.isAdmin()`).
- `safeCount` 는 `ApplicantCounter.count` 의 `GateUnavailableException` 을 잡아 -1 을 돌려준다 (로그용).
- 수동 추첨은 상태를 앞에서 검사하지만, 최종 방어선은 `DrawTransaction` 의 조건부 UPDATE 다 (검사와 실행 사이에 스케줄러가 먼저 추첨하면 `skipped` → 409).
- 건수 비교를 트랜잭션 밖에서 하는 이유: 불일치 시 아무것도 쓰지 않으므로 트랜잭션이 필요 없고, Redis 호출을 DB 트랜잭션 안에 넣지 않기 위해서다.

#### `DrawTransaction` — 추첨 단일 트랜잭션 (구현 결정)
`DrawService` 안에서 `@Transactional` 메서드를 자기 호출하면 프록시가 적용되지 않으므로 별도 빈으로 분리한다.

```java
@Component
@RequiredArgsConstructor
public class DrawTransaction {

    private final DropSnapshotRepository snapshots;
    private final EntryRepository entries;
    private final DrawResultRepository results;
    private final SeedGenerator seedGenerator;
    private final Drawer drawer = new Drawer();

    @Transactional
    public DrawOutcome execute(Long dropId, Instant now) {
        // ① CLOSING → DRAWN 조건부 UPDATE: 행 잠금 획득. 다른 인스턴스는 여기서 대기 후 false
        if (!snapshots.markDrawnIfClosing(dropId)) {
            return DrawOutcome.skipped();
        }
        DropSnapshot snapshot = snapshots.findById(dropId).orElseThrow();

        // ② 결정적 순서로 응모자 조회
        List<Long> applicants = entries.findUserIdsOrderBySeq(dropId);

        // ③ 시드 셔플
        long seed = seedGenerator.next();
        Set<Long> winners = drawer.draw(applicants, snapshot.getWinnerCount(), seed);

        // ④ 벌크 상태 반영
        entries.markResults(dropId, winners);

        // ⑤ 추첨 기록 (drop_id UK → 중복 추첨 최종 방어선)
        DrawResult result = results.save(DrawResult.of(dropId, seed, applicants.size(), winners.size(), now));

        return new DrawOutcome(true, new DrawCompletedPayload(dropId, result.getApplicantCount(), result.getWinnerCount(), now));
    }
}
```

**트랜잭션 범위**
| 포함 | 제외 |
|------|------|
| ① DRAWN 전이 · ② 응모자 조회 · ④ WON/LOST 벌크 UPDATE · ⑤ draw_results INSERT | Redis 건수 비교 (이전), `draw.completed` 발행 (이후) |

- 설계 문서는 "추첨 → draw_results → DRAWN" 순서지만, **DRAWN 전이를 트랜잭션 첫 문장으로 당겼다** (구현 결정). 조건부 UPDATE 가 행 잠금을 잡아 두 인스턴스가 동시에 같은 드로우를 추첨하는 것을 막고, 어느 단계에서 실패해도 롤백되어 CLOSING 으로 돌아간다. 결과는 설계와 동일 (6장 참고).
- 롤백 후 재시도 시 새 시드를 쓴다. 커밋 전이라 외부에 결과가 노출되지 않았으므로 공정성 문제 없음 ([draw.md](../../design/05-features/draw.md) 6장).

#### `DrawQueryService`
```java
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DrawQueryService {
    private final DrawResultRepository results;

    public DrawSummaryResult summary(Long dropId) {
        return results.findByDropId(dropId)
                .map(DrawSummaryResult::from)
                .orElseThrow(() -> new ApplicationException(DrawErrorCode.NOT_COMPLETED));
    }
}
```
- 내 결과 조회(`GET /api/entries/drops/{dropId}/me`)는 4단계 `EntryQueryService` 가 이미 `entries.status` 를 반환하므로 추첨 후 자동으로 `WON / LOST` 가 보인다. 5단계에서는 테스트만 추가한다 (6장 설계 수정 필요 참고).

---

### 3.3 entry-service — infrastructure

#### Flyway `V2__create_draw_results.sql`
```sql
CREATE TABLE draw_results (
    id              BIGSERIAL    PRIMARY KEY,
    drop_id         BIGINT       NOT NULL,
    seed            BIGINT       NOT NULL,
    applicant_count INT          NOT NULL,
    winner_count    INT          NOT NULL,
    drawn_at        TIMESTAMPTZ  NOT NULL,
    published       BOOLEAN      NOT NULL DEFAULT FALSE,   -- 구현 결정 (설계 수정 필요)
    CONSTRAINT uk_draw_results_drop_id UNIQUE (drop_id),
    CONSTRAINT ck_draw_results_counts CHECK (winner_count >= 0 AND winner_count <= applicant_count)
);

CREATE INDEX idx_draw_results_unpublished ON draw_results (drop_id) WHERE published = FALSE;
```

#### 조건부 · 벌크 쿼리 (`DropSnapshotJpaRepository`, `EntryJpaRepository`)
```java
public interface DropSnapshotJpaRepository extends JpaRepository<DropSnapshotJpaEntity, Long> {

    @Query("select s.dropId from DropSnapshotJpaEntity s where s.status = 'OPEN' and s.closeAt <= :now")
    List<Long> findDueOpenIds(@Param("now") Instant now);

    @Modifying
    @Query(value = """
        UPDATE drop_snapshots SET status = 'CLOSING'
         WHERE drop_id = :dropId AND status = 'OPEN' AND close_at <= :now
        """, nativeQuery = true)
    int markClosingIfOpen(@Param("dropId") Long dropId, @Param("now") Instant now);

    @Modifying
    @Query(value = """
        UPDATE drop_snapshots SET status = 'DRAWN'
         WHERE drop_id = :dropId AND status = 'CLOSING'
        """, nativeQuery = true)
    int markDrawnIfClosing(@Param("dropId") Long dropId);

    List<DropSnapshotJpaEntity> findByStatus(String status);
}

public interface EntryJpaRepository extends JpaRepository<EntryJpaEntity, Long> {

    long countByDropId(Long dropId);

    @Query(value = "SELECT user_id FROM entries WHERE drop_id = :dropId ORDER BY entry_seq", nativeQuery = true)
    List<Long> findUserIdsOrderBySeq(@Param("dropId") Long dropId);

    @Modifying
    @Query(value = """
        UPDATE entries SET status = 'WON', updated_at = now()
         WHERE drop_id = :dropId AND status = 'APPLIED' AND user_id IN (:userIds)
        """, nativeQuery = true)
    int markWon(@Param("dropId") Long dropId, @Param("userIds") Collection<Long> userIds);

    @Modifying
    @Query(value = """
        UPDATE entries SET status = 'LOST', updated_at = now()
         WHERE drop_id = :dropId AND status = 'APPLIED'
        """, nativeQuery = true)
    int markRemainingLost(@Param("dropId") Long dropId);
}
```

```java
// EntryRepositoryImpl
@Override
public void markResults(Long dropId, Set<Long> winnerUserIds) {
    // IN 절 파라미터 수 제한(PostgreSQL 32767) 대비 1,000건 단위 분할 (구현 결정)
    for (List<Long> chunk : Chunks.of(winnerUserIds, 1_000)) {
        jpa.markWon(dropId, chunk);
    }
    jpa.markRemainingLost(dropId);     // WON 처리 후 남은 APPLIED 전부 LOST
}
```
- `markClosingIfOpen` / `markDrawnIfClosing` 의 반환값(영향 행 수) `== 1` 을 `boolean` 으로 바꿔 domain 포트에 돌려준다.
- 구현 메모 (v1.4): JPA 쿼리 인자도 원시 `long`. `findByStatus` 인자는 `SnapshotStatus`(enum 필드). JPQL 상태 비교는 enum 전체 이름. 당첨자 분할은 `subList` 1,000건(빈 집합이면 LOST 만). `DrawResultRepositoryImpl.save` 는 `saveAndFlush` — drop_id UK 위반을 save 호출에서 드러낸다. V2 id 는 V1 과 같은 `GENERATED ALWAYS AS IDENTITY`.
- 벌크 UPDATE 는 `@Modifying(flushAutomatically = true, clearAutomatically = true)` 로 앞선 변경을 먼저 flush 하고 영속성 컨텍스트를 비운다 — 같은 트랜잭션에서 먼저 읽은 엔티티가 낡은 APPLIED 를 돌려주지 않게 (T5-15 보강).
- `status = 'APPLIED'` 조건 덕분에 같은 트랜잭션이 재실행되어도 이미 확정된 행을 바꾸지 않는다.

#### Redis 어댑터
```java
// RedisEntryGate (수정)
@Override
public void close(Long dropId) {
    redis.opsForHash().put(EntryKeys.meta(dropId), "state", "CLOSED");
}

// RedisApplicantCounter (신규)
@Component
@RequiredArgsConstructor
public class RedisApplicantCounter implements ApplicantCounter {
    private final StringRedisTemplate redis;

    @Override
    public long count(Long dropId) {
        Long size = redis.opsForSet().size(EntryKeys.applicants(dropId));
        return size == null ? 0 : size;
    }
}
```
```java
// RedisEntryGate (수정) — 6개 키 EXPIRE. 파이프라인 1회, DataAccessException → GateUnavailableException (4단계 call() 래퍼)
@Override
public void expireAfterDraw(Long dropId, Duration ttl) {
    call(() -> redis.executePipelined((RedisCallback<Object>) c -> {
        for (String key : EntryRedisKeys.all(dropId)) c.keyCommands().expire(key.getBytes(UTF_8), ttl.toSeconds());
        return null;
    }));
}
```
- `EntryRedisKeys.all(dropId)` = meta, remaining, applicants, seq, requests, persisted. 없는 키(예: 응모 0건이면 applicants·seq 없음)는 EXPIRE 가 0 을 돌려줄 뿐 오류가 아니다.
- `RedisApplicantCounter.count` 도 `DataAccessException` → `GateUnavailableException` 으로 감싼다.
- `apply.lua` 는 4단계에서 이미 `meta.state == OPEN` 과 `closeAt` 을 함께 검사하므로 수정하지 않는다. `close()` 이전에 장애가 나도 시각 검사로 마감 후 응모가 막힌다 ([draw.md](../../design/05-features/draw.md) 5.1 읽는 법).

#### `KafkaDrawEventPublisher`
```java
@Component
@RequiredArgsConstructor
public class KafkaDrawEventPublisher implements DrawEventPublisher {
    private final KafkaTemplate<String, Object> kafka;

    @Override
    public void publishCompleted(DrawCompletedPayload p) {
        DrawCompleted message = new DrawCompleted(p.dropId(), p.applicantCount(), p.winnerCount(), p.drawnAt());
        try {
            kafka.send(Topics.DRAW_COMPLETED, String.valueOf(p.dropId()), message).get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new EventPublishException("draw.completed", e);   // infrastructure 예외 → DrawService 가 로그 후 재발행 대상
        }
    }
}
```
- 파티션 키 `dropId` ([architecture.md](../../design/02-architecture/architecture.md) 4장).

#### `SecureRandomSeedGenerator`, `SchedulingConfig`
```java
@Component
public class SecureRandomSeedGenerator implements SeedGenerator {
    private final SecureRandom random = new SecureRandom();
    @Override public long next() { return random.nextLong(); }
}

@Configuration
@EnableScheduling
public class SchedulingConfig {
    @Bean Clock clock() { return Clock.system(ZoneId.of("Asia/Seoul")); }
}
```

#### 설정 (`application.yml` 추가)
```yaml
entry:
  draw:
    interval-ms: 5000          # 추첨 스케줄러 주기
    closing-warn-threshold: 5m # CLOSING 장기화 경고 기준 (경고 후 관리자 수동 추첨, D5-1)
    key-ttl-after-draw: 1d     # 추첨 커밋 후 응모 Redis 키 만료 (E-42)
```

---

### 3.4 entry-service — presentation

#### `DrawScheduler`
```java
@Component
@RequiredArgsConstructor
public class DrawScheduler {
    private final DrawService drawService;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${entry.draw.interval-ms}")
    public void run() {
        Instant now = clock.instant();
        drawService.closeDueDrops(now);
        drawService.drawReadyDrops(now);
        drawService.republishUnpublished();
    }
}
```
- `fixedDelay` 를 쓰는 이유: 이전 실행이 끝난 뒤 간격을 두므로 한 인스턴스 안에서 실행이 겹치지 않는다. 인스턴스 간 중복은 조건부 UPDATE 가 막는다 (분산 락 불필요).

#### `DrawResultController`
```java
@RestController
@RequestMapping("/api/entries/drops/{dropId}")
@RequiredArgsConstructor
public class DrawResultController {
    private final DrawQueryService queryService;

    @GetMapping("/result")
    public DrawSummaryResponse summary(@PathVariable Long dropId) {
        return DrawSummaryResponse.from(queryService.summary(dropId));
    }
}

public record DrawSummaryResponse(Long dropId, int applicantCount, int winnerCount, String seed, Instant drawnAt) {
    public static DrawSummaryResponse from(DrawSummaryResult r) {
        return new DrawSummaryResponse(r.dropId(), r.applicantCount(), r.winnerCount(), Long.toString(r.seed()), r.drawnAt());
    }
}
```
- `seed` 를 문자열로 내보낸다 (구현 결정): JavaScript `Number` 는 2^53 을 넘는 long 을 정확히 표현하지 못한다.
- 추첨 전 조회 → `ENTRY_DRAW_NOT_COMPLETED` (409).

#### `AdminDrawController` (D5-1)
```java
@RestController
@RequestMapping("/api/entries/admin/drops/{dropId}")
@RequiredArgsConstructor
public class AdminDrawController {
    private final DrawService drawService;
    private final Clock clock;

    @PostMapping("/draw")
    public DrawSummaryResponse draw(@PathVariable Long dropId,
                                    @RequestHeader("X-User-Id") Long userId,
                                    @RequestHeader("X-User-Role") String role) {
        return DrawSummaryResponse.from(
                drawService.drawManually(new ManualDrawCommand(new Requester(userId, role), dropId), clock.instant()));
    }
}
```
- 운영 절차: `CLOSING` 장기화 WARN → DLT 재처리로 해소되는지 먼저 확인 → 해소되지 않으면 이 API 호출. 응답은 결과 조회와 같은 `DrawSummaryResponse`.

| Method | Path | 권한 | 응답 |
|--------|------|------|------|
| POST | `/api/entries/admin/drops/{dropId}/draw` | ADMIN | 200 추첨 요약 / 403 / 404 / 409 `NOT_CLOSING`·`ALREADY_DRAWN` |
| GET | `/api/entries/drops/{dropId}/me` | USER | 4단계 구현 재사용 (APPLIED / WON / LOST) |
| GET | `/api/entries/drops/{dropId}/result` | 공개 (구현 결정, 6장) | 응모자 수, 당첨 수, 시드, 추첨 시각 |

---

### 3.5 product-service

#### `DropService.markDrawn` (수정)
```java
public record MarkDrawnCommand(Long dropId) {}

@Transactional
public void markDrawn(MarkDrawnCommand command) {
    Drop drop = dropRepository.findById(command.dropId())
            .orElseThrow(() -> new ApplicationException(ProductErrorCode.DROP_NOT_FOUND));
    if (drop.isDrawn()) {
        return;                                 // 멱등: 재전달 시 무시
    }
    drop.markDrawn();                           // 2단계 구현: void
    dropRepository.save(drop);                  // 변경 감지 미사용 → 명시 save (E-30)
}
```

#### `DrawCompletedListener`
```java
@Component
@RequiredArgsConstructor
public class DrawCompletedListener {
    private final DropService dropService;

    @KafkaListener(topics = Topics.DRAW_COMPLETED, groupId = "product-service")
    public void on(DrawCompleted message) {
        dropService.markDrawn(new MarkDrawnCommand(message.dropId()));
    }
}
```
- 드로우가 없으면(`PRODUCT_DROP_NOT_FOUND`) 재시도해도 해결되지 않으므로 에러 로그 후 커밋한다 (구현 결정). `ApplicationException` 전체를 not-retryable 로 등록하지 않고, `DefaultErrorHandler` 의 분류 함수에서 **에러 코드가 `DROP_NOT_FOUND` 인 경우만** 재시도하지 않는다 (E-30). 그 밖의 예외는 재시도 후 `draw.completed.DLT`.

---

### 3.6 E2E 시나리오 테스트 구성 (구현 결정)

**선택지**
| 방식 | 내용 | 판단 |
|------|------|------|
| A. 단일 JVM 에서 컨텍스트 여러 개 | 한 테스트가 6개 서비스의 `SpringApplication` 을 띄움 | ❌ 모든 모듈에 `application.yml` 이 같은 이름으로 있어 클래스패스 충돌. 빈·자동 설정 충돌 정리 비용이 큼 |
| B. Testcontainers `ComposeContainer` | 서비스 이미지를 compose 로 띄우고 테스트 | ⏳ 서비스 Dockerfile 은 8단계 산출물. 8단계 이후 이 방식으로 전환 |
| **C. 별도 `e2e-test` 모듈 + 실행 스크립트** | 인프라는 `docker compose`, 서비스는 `bootRun` 으로 띄우고, 테스트는 Gateway 로 HTTP 만 호출 | ✅ **채택**. 지금 있는 것만으로 가능, 서비스 코드에 의존하지 않음 |

**구성**
- `e2e-test` 모듈: `RestClient` + JUnit 5 + Awaitility. 서비스 모듈에 의존하지 않는다 (HTTP 계약만 검증).
- `@Tag("e2e")` 로 분리하고 기본 `./gradlew build` 에서 제외: `tasks.test { useJUnitPlatform { excludeTags 'e2e' } }`, 별도 `e2eTest` 태스크.
- `scripts/e2e.sh`: `docker compose up -d` → 각 서비스 `./gradlew :{svc}:bootRun --args='--spring.profiles.active=e2e'` 백그라운드 실행 → actuator health 대기 → `./gradlew :e2e-test:e2eTest` → 종료 시 프로세스 정리.
- `e2e` 프로필 설정
  - product: 드로우 최소 오픈 지연을 5초로 단축 (`product.drop.min-lead-time: 5s`, 2단계 설정값 재사용)
  - queue: 입장 주기 200ms
  - entry: 추첨 주기 1초
  - user: 관리자 계정 시드 (1단계에서 정한 방식 사용)

**시나리오 (`DropFlowE2ETest`)**
1. 관리자 로그인 → 상품 등록 → 드로우 등록 (정원 M=5, 당첨 K=2, openAt=now+5s, closeAt=now+20s)
2. 사용자 7명 가입·로그인
3. openAt 이후 7명 대기열 진입 → `ADMITTED` 될 때까지 폴링
4. 7명 응모 → 5명 `202`, 2명 `409 SOLD_OUT`
5. closeAt 이후 `GET /result` 가 200 이 될 때까지 폴링 (최대 30초)
6. 검증: `applicantCount=5`, `winnerCount=2`, 응모자 5명의 `/me` 가 WON 2 · LOST 3, 드로우 상세 상태 `DRAWN`

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ/NFR |
|----|-----------|---------------|-------------------------------|---------------|
| T5-01 | domain 단위 | `DrawerTest` | G: 응모자 10명, K=3, seed=42 / W: 두 번 추첨 / T: 결과 동일 | NFR-04 |
| T5-02 | domain 단위 | `DrawerTest` | G: 같은 목록, seed 42 와 43 / W: 추첨 / T: 결과가 다름 (고정 시드로 확인) | NFR-04 |
| T5-03 | domain 단위 | `DrawerTest` | G: 응모자 3명, K=5 / W: 추첨 / T: 3명 전원 당첨 | REQ-ENTRY-05 |
| T5-04 | domain 단위 | `DrawerTest` | G: 응모자 0명, K=5 / W: 추첨 / T: 빈 집합 | REQ-ENTRY-05 |
| T5-05 | domain 단위 | `DrawerTest` | G: 응모자 100명, K=10 / W: 추첨 / T: 크기 10, 모두 입력에 포함, 입력 리스트 불변 | REQ-ENTRY-05 |
| T5-06 | domain 단위 | `DrawerTest` | G: K=0 / W: 추첨 / T: `DomainException` | - |
| T5-07 | domain 단위 | `DropSnapshotTest` | G: closeAt=12:00 / W: `isDue(11:59:59)`, `isDue(12:00)` / T: false, true | REQ-ENTRY-05 |
| T5-08 | domain 단위 | `DropSnapshotTest` | G: CLOSING, closeAt=12:00, 기준 5분 / W: `isClosingTooLong(12:05)`, `(12:05:01)` / T: false, true | NFR-03 |
| T5-09 | application 단위 (Fake) | `DrawServiceTest` | G: 마감 지난 OPEN 드로우 / W: `closeDueDrops` / T: 상태 CLOSING, Fake Gate 에 close 기록 | REQ-ENTRY-05 |
| T5-10 | application 단위 (Fake) | `DrawServiceTest` | G: CLOSING, Redis 1000 / DB 987 / W: `drawReadyDrops` / T: 추첨 안 함, 발행 없음 | NFR-03 |
| T5-11 | application 단위 (Fake) | `DrawServiceTest` | G: CLOSING, 건수 일치, 고정 시드 / W: `drawReadyDrops` / T: 추첨 1회, 발행 1회, published=true | REQ-ENTRY-05 |
| T5-12 | application 단위 (Fake) | `DrawServiceTest` | G: Fake publisher 예외 / W: 추첨 / T: 결과 저장·DRAWN 유지, published=false. 다음 `republishUnpublished` 에서 발행 | NFR-03 |
| T5-13 | application 단위 (Fake) | `DrawServiceTest` | G: CLOSING 이 기준 초과, 건수 불일치 / W: `drawReadyDrops` / T: WARN 로그 1건 (`OutputCaptureExtension`) | NFR-03 |
| T5-14 | infra 통합 (PostgreSQL) | `DropSnapshotRepositoryImplTest` | G: OPEN 드로우 / W: 스레드 2개 동시 `markClosingIfOpen` / T: true 정확히 1개 | REQ-ENTRY-05 |
| T5-15 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: 응모 2,500건, 당첨 1,200명 / W: `markResults` / T: WON 1,200, LOST 1,300, APPLIED 0 (분할 처리 확인) | REQ-ENTRY-05 |
| T5-16 | infra 통합 (PostgreSQL) | `EntryRepositoryImplTest` | G: entry_seq 를 섞어서 저장 / W: `findUserIdsOrderBySeq` / T: entry_seq 오름차순 | NFR-04 |
| T5-17 | infra 통합 (PostgreSQL) | `DrawTransactionTest` | G: CLOSING 드로우, 건수 일치 / W: 스레드 2개 동시 `execute` / T: `draw_results` 1건, drawn=true 1개 | REQ-ENTRY-05 |
| T5-18 | infra 통합 (PostgreSQL) | `DrawTransactionTest` | G: `markResults` 에서 예외 주입 / W: `execute` / T: 롤백 → 상태 CLOSING, entries 모두 APPLIED, draw_results 없음 | NFR-03 |
| T5-19 | infra 통합 (Redis) | `RedisEntryGateCloseTest` | G: OPEN 드로우, 토큰 보유 / W: `close` 후 `tryApply` / T: `CLOSED` | REQ-ENTRY-05 |
| T5-20 | infra 통합 (Kafka) | `KafkaDrawEventPublisherTest` | W: `publishCompleted` / T: `draw.completed` 토픽에 키=dropId 메시지 1건 | - |
| T5-21 | 통합 (전체 entry 컨텍스트) | `DrawSchedulerIntegrationTest` | G: 응모 100건 저장, 마감 지남 / W: `DrawScheduler` 빈 2개(같은 컨텍스트에 두 번째 인스턴스 수동 생성)를 동시 실행 / T: `draw_results` 1건, 발행 1건 | REQ-ENTRY-05 |
| T5-22 | 통합 (전체 entry 컨텍스트) | `DrawSchedulerIntegrationTest` | G: Redis 응모자 100, DB 90 / W: 스케줄 1회 → 나머지 10건 저장 → 스케줄 1회 / T: 1회차 추첨 없음, 2회차 추첨 | NFR-03 |
| T5-23 | presentation 슬라이스 | `DrawResultControllerTest` | G: 추첨 전 / 후 / W: `GET /result` / T: 409 `ENTRY_DRAW_NOT_COMPLETED` / 200 + seed 문자열 | REQ-ENTRY-06 |
| T5-24 | 통합 (entry 컨텍스트) | `EntryQueryAfterDrawTest` | G: 추첨 완료 / W: `GET /me` / T: 당첨자 WON, 낙첨자 LOST | REQ-ENTRY-06 |
| T5-25 | product 통합 (Kafka + PostgreSQL) | `DrawCompletedListenerTest` | G: drawn=false 드로우 / W: `draw.completed` 2번 수신 / T: drawn=true, 예외 없음, 상세 상태 DRAWN | REQ-DROP-03 |
| T5-26 | 아키텍처 | `ArchitectureTest` (entry, product) | 새 클래스 포함 R1~R7 통과 (`DrawService` public 시그니처에 domain 타입 없음) | - |
| T5-27 | E2E | `DropFlowE2ETest` | 3.6 시나리오 | REQ-DROP-*, REQ-QUEUE-*, REQ-ENTRY-* |
| T5-28 | application 단위 (Fake) | `DrawServiceTest` | G: CLOSING, Redis 10 / DB 9, ADMIN / W: `drawManually` / T: DB 9명 기준 추첨, 발행 1회, 결과 applicantCount=9, WARN 로그(redis·db 수) | NFR-03 (D5-1) |
| T5-29 | application 단위 (Fake) | `DrawServiceTest` | G: USER 역할 / OPEN 드로우 / DRAWN 드로우 / 없는 dropId / W: `drawManually` / T: 403 `FORBIDDEN` / 409 `ENTRY_DRAW_NOT_CLOSING` / 409 `ENTRY_DRAW_ALREADY_DRAWN` / 404, 어느 경우도 추첨·발행 없음 | D5-1 |
| T5-30 | application 단위 (Fake) | `DrawServiceTest` | G: 건수 일치 / W: `drawReadyDrops` / T: Fake Gate 에 `expireAfterDraw(dropId, 1d)` 기록. Gate 가 `GateUnavailableException` 을 던져도 추첨·발행은 유지 | E-42 |
| T5-31 | infra 통합 (Redis) | `RedisEntryGateCloseTest` | G: 응모 1건 있는 드로우(6개 키) / W: `expireAfterDraw(1d)` / T: 6개 키 TTL 이 0 초과 86400 이하. 응모 0건 드로우도 예외 없음 | E-42 |
| T5-32 | presentation 슬라이스 | `AdminDrawControllerTest` | G: ADMIN 헤더 / 헤더 없음 / W: `POST /api/entries/admin/drops/1/draw` / T: 200 + seed 문자열 / 400 `COMMON_MISSING_USER_HEADER` (0단계 에러 계약) | D5-1 |
| T5-33 | domain 단위 | `DrawResultTest` | G: applicantCount=-1 / winnerCount=-1 / winnerCount > applicantCount / 정상값 / W: `DrawResult.of`, `markPublished` / T: `DomainException` ×3 / 생성 성공·published=false, markPublished 는 published=true 인 새 인스턴스(원본 불변) | REQ-ENTRY-05 |
| T5-34 | infra 통합 (PostgreSQL) | `DrawResultRepositoryImplTest` | G: DrawResult / W: save → findByDropId, findUnpublished, markPublished(2회), 같은 drop_id 재저장 / T: 필드 왕복(음수 seed, μs Instant), 미발행만 조회, 멱등, `DataIntegrityViolationException` | REQ-ENTRY-05 |

---

## 5. 커밋 계획

| # | 범위 | 커밋 메시지 |
|---|------|-------------|
| 0 | docs | `docs: 5단계 착수 전 결정 반영`<br>- D5-1(관리자 수동 추첨), D5-4(추첨 후 키 만료), E-30, 에러 코드 계약 정렬<br>- draw.md, class-diagram.md, 플랜·백로그 갱신 |
| 1 | domain | `feat(entry): 추첨 도메인 모델과 Drawer 추가`<br>- Drawer: entry_seq 정렬 목록 + K + 시드 셔플 추첨<br>- DrawResult, DropSnapshot.isDue / isClosingTooLong / isClosing / isDrawn<br>- DrawResultRepository 포트(신규)<br>- T5-01 ~ T5-08, T5-33 |
| 2 | infra (DB) | `feat(entry): 추첨 영속성 구현`<br>- DropSnapshotRepository·EntryRepository 포트 확장 + Impl + 테스트 Fake 갱신<br>- V2 draw_results 마이그레이션, DrawResult 영속(JpaEntity·Mapper·Impl, `DrawResult.restore`)<br>- 조건부 UPDATE(OPEN→CLOSING, CLOSING→DRAWN), 벌크 WON/LOST<br>- T5-14 ~ T5-16, T5-34 |
| 3 | infra (Redis, Kafka) | `feat(entry): 마감 처리와 draw.completed 발행 구현`<br>- 포트 ApplicantCounter, DrawEventPublisher, SeedGenerator(+ DrawCompletedPayload), EntryGate.close·expireAfterDraw 와 어댑터를 함께<br>- RedisEntryGate.close·expireAfterDraw, RedisApplicantCounter<br>- KafkaDrawEventPublisher, SecureRandomSeedGenerator<br>- T5-19, T5-20, T5-31 |
| 4 | application | `feat(entry): 마감·추첨 유스케이스 구현`<br>- DrawService(마감 잠금, 건수 비교, 재발행, 관리자 수동 추첨, 추첨 후 키 만료), DrawTransaction<br>- Fake 포트 갱신(장원영)<br>- T5-09 ~ T5-13, T5-28 ~ T5-30 (Fake 포트), T5-17 · T5-18 (DrawTransaction + PostgreSQL) |
| 5 | presentation | `feat(entry): 추첨 스케줄러와 결과 조회·관리자 추첨 API 추가`<br>- DrawScheduler, DrawResultController, AdminDrawController<br>- T5-21 ~ T5-24, T5-32 |
| 6 | product | `feat(product): draw.completed 수신 시 드로우 추첨 완료 반영`<br>- DrawCompletedListener, DropService.markDrawn (멱등), KafkaConsumerConfig(DROP_NOT_FOUND 비재시도)<br>- T5-25 |
| 7 | e2e | `test: 드로우 전체 흐름 E2E 테스트 추가`<br>- e2e-test 모듈, scripts/e2e.sh, e2e 프로필<br>- T5-27 |
| 8 | docs | `docs: 5단계 구현 결과 반영`<br>- 설계 수정 필요 항목 반영, 플랜 진행 현황 갱신 |

---

## 6. 결정 필요 · 리스크

### 결정 필요
| ID | 항목 | 선택지 | 기본안 |
|----|------|--------|--------|
| D5-1 | 건수 불일치 장기화 시 처리 | (a) 경고 후 관리자 수동 확정 API (b) 일정 시간 후 DB 기준 강제 추첨 | **✅ 확정(사용자 결정, 2026-11-17): (a), 5단계에 포함.** `POST /api/entries/admin/drops/{dropId}/draw` (ADMIN): 건수 비교를 건너뛰고 DB 응모 기준으로 추첨 (3.2 `drawManually`, 3.4). Redis 에만 남은 응모(보상까지 실패, 저장 DLT 미처리)는 빠지며, 별도 Redis∖DB 대조 작업(E-41 개선안 2)은 만들지 않는다 |
| D5-2 | 추첨 요약 API 공개 범위 | (a) 공개 (b) 로그인 사용자만 | (a) 공개 — 시드 공개로 결과 검증 가능성 제공. Gateway 인증 제외 경로 추가 필요 |
| D5-3 | E2E 실행 방식 | 3.6 의 A / B / C | C (8단계 이후 B 로 전환) |
| D5-4 | 응모 Redis 키 정리 (E-42) | (1) initialize 에서 `closeAt + N일` 만료 (2) 추첨 커밋 후 EXPIRE (3) 둘 다 | **확정(기본안, 2026-11-17): (2)** — 추첨이 끝난 드로우만 정리하므로 건수 비교 근거가 먼저 사라질 위험이 없다. 1일(`key-ttl-after-draw`). 안전망 TTL(1)은 `requests`·`persisted`·`seq` 가 apply/claim 시점에 생겨 Lua 마다 EXPIREAT 를 넣어야 해서 이번 범위에서 제외 — 백로그 E-42 에 남김 |

### 설계 수정 필요
| 대상 문서 | 내용 |
|-----------|------|
| [erd.md](../../design/06-erd/erd.md) entry_db | `draw_results.published BOOLEAN` 추가 — 커밋 후 `draw.completed` 발행 실패 시 재발행 대상 식별용 → 설계 반영 완료 (2026-11-16) |
| [draw.md](../../design/05-features/draw.md) 3장 | 추첨 트랜잭션 순서: `CLOSING → DRAWN` 조건부 UPDATE 를 **첫 문장**으로 (다중 인스턴스 동시 추첨 방지용 행 잠금). 발행 실패 시 재발행(스케줄러) 추가 → 설계 반영 완료 (2026-11-16) |
| [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 1.2 | `DrawTransaction`, `SeedGenerator`, `DrawOutcome` 추가. `DrawService` 메서드명 `drawIfReady` → `drawReadyDrops` + `republishUnpublished`. `DrawResultController.myResult` 제거 (4단계 `EntryController` 의 `/me` 재사용) → 설계 반영 완료 (2026-11-16) |
| [architecture.md](../../design/02-architecture/architecture.md) 6장 | D5-2 가 (a) 이면 인증 제외 경로에 `GET /api/entries/drops/*/result` 추가 → 설계 반영 완료 (2026-11-16) |

### 리스크
- **응모자 수가 매우 큰 경우**: 전체 userId 를 메모리에 올려 셔플한다. M 이 수십만 이상이면 부분 셔플(K 개 인덱스만 뽑기)로 바꿔야 한다. 현재 범위(수천~수만)는 문제 없음.
- **추첨 트랜잭션 길이**: 응모자 조회 + 벌크 UPDATE 가 한 트랜잭션이다. 응모자가 많으면 `drop_snapshots` 행 잠금 시간이 길어지지만, 이 행을 동시에 쓰는 다른 경로가 없어 영향은 추첨 중복 방지 대기뿐이다.
- **E2E 불안정성**: 시각 기반 시나리오라 CI 환경에서 느리면 실패할 수 있다. Awaitility 타임아웃을 넉넉히(30초) 두고, 기본 빌드에서는 제외한다.
- **추첨 후 키 만료 실패**: Redis 장애로 `expireAfterDraw` 가 실패하면 그 드로우의 키는 만료 없이 남는다 (WARN 1줄). 재시도하지 않는다 — 정합성 영향이 없고 메모리 문제는 드로우 수백 개 단위에서나 생긴다 (E-42).
- **수동 추첨으로 빠지는 응모**: 202 를 받았지만 저장이 DLT 에 머문 사용자는 수동 추첨에서 제외된다. 그래서 운영 절차는 DLT 재처리를 먼저 하고 API 를 부르는 순서다 (3.4). 보상까지 실패한 사용자는 503 을 받았으므로 응모가 성립하지 않은 것으로 본다.
- **스케줄러 시계**: 추첨 판단은 entry-service 인스턴스의 `Clock` 기준이다. 인스턴스 간 시계 차이가 크면 마감 잠금 시점이 몇 초 달라질 수 있으나, 응모 차단은 Redis Lua 의 시각 검사가 담당하므로 정합성에는 영향 없음.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 테스트 ID |
|----------------|-----------|
| 단위(`Drawer`): 같은 시드 → 같은 결과, K ≥ 응모자 수 → 전원 당첨, 응모자 0 → 당첨 0 | T5-01 ~ T5-06 |
| 통합: 스케줄러 2개 동시 실행 → `draw_results` 1건 | T5-14, T5-17, T5-21 |
| 통합: DB 응모 수가 부족한 상태 → 추첨 대기, 저장 완료 후 다음 주기에 추첨 | T5-10, T5-22 |
| 통합: 마감 후 응모 → `CLOSED` | T5-19 |
| 통합: 추첨 후 product 드로우 상태 `DRAWN` | T5-25, T5-27 |
| E2E: 드로우 등록 → 대기열 → 응모 → 마감 → 추첨 → 결과 조회 | T5-27 |
| (추가) 추첨 실패 시 롤백·재시도 가능 | T5-18 |
| (추가) 발행 실패 시 재발행 | T5-12 |
| (추가) 결과 조회 API | T5-23, T5-24 |
| 공통: `./gradlew build` 통과 (ArchUnit 포함) | T5-26 |
| 공통: 설계 수정 필요 항목 반영 + 변경 이력 | 커밋 #8 |
| (추가) 관리자 수동 추첨 (D5-1) | T5-28, T5-29, T5-32 |
| (추가) 추첨 후 응모 키 만료 (E-42) | T5-30, T5-31 |
| 공통: D5-1 ~ D5-4 결정 및 문서 반영 | 커밋 #0 (착수 전), #8 |
