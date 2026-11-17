package com.queuedrop.ranking.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import ch.qos.logback.classic.Level;
import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.ranking.application.dto.DropRankResult;
import com.queuedrop.ranking.application.dto.MyRankResult;
import com.queuedrop.ranking.application.dto.UserRankResult;
import com.queuedrop.ranking.application.dto.WeeklyRankingResult;
import com.queuedrop.ranking.application.exception.RankingErrorCode;
import com.queuedrop.ranking.application.fake.InMemoryDisplayNameRepository;
import com.queuedrop.ranking.application.fake.InMemoryDropWindowRepository;
import com.queuedrop.ranking.application.fake.InMemoryRankingRepository;
import com.queuedrop.ranking.application.fake.InMemoryRankingRepository.TopCall;
import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** RankingQueryService 단위 (Fake 저장소, 고정 Clock). 읽기 경로 (단계 문서 3.2) */
class RankingQueryServiceTest {

    /** 2026-11-18(수) 12:00 KST → 2026-W47 */
    private static final Instant NOW = OffsetDateTime.parse("2026-11-18T12:00:00+09:00").toInstant();
    private static final WeekKey THIS_WEEK = new WeekKey(2026, 47);
    private static final Instant OPEN_UNTIL = NOW.plus(Duration.ofDays(1));
    private static final Instant CLOSED_AT = NOW.minus(Duration.ofHours(1));

    private InMemoryRankingRepository rankingRepository;
    private InMemoryDisplayNameRepository displayNameRepository;
    private InMemoryDropWindowRepository dropWindowRepository;
    private RankingQueryService service;
    private LogCapture logs;

    @BeforeEach
    void setUp() {
        rankingRepository = new InMemoryRankingRepository();
        displayNameRepository = new InMemoryDisplayNameRepository();
        dropWindowRepository = new InMemoryDropWindowRepository();
        service = serviceAt(NOW);
        logs = new LogCapture(RankingQueryService.class);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private RankingQueryService serviceAt(Instant now) {
        return new RankingQueryService(rankingRepository, displayNameRepository, dropWindowRepository,
                Clock.fixed(now, ZoneOffset.UTC));
    }

    private void givenDrop(long dropId, long score, Instant closeAt) {
        rankingRepository.givenScore(RankingBoard.DROP_POPULAR, null, dropId, score);
        if (closeAt != null) {
            dropWindowRepository.givenCloseAt(dropId, closeAt);
        }
    }

    /** dropId 1..closedCount 는 종료(점수 높은 순), 그 다음 id 하나가 진행 중 (점수 최저) */
    private long givenClosedThenOneOpen(int closedCount) {
        long base = 10_000;
        LongStream.rangeClosed(1, closedCount).forEach(id -> givenDrop(id, base - id, CLOSED_AT));
        long openId = closedCount + 1L;
        givenDrop(openId, 1, OPEN_UNTIL);
        return openId;
    }

    private static void assertErrorCode(ThrowingCallable call, RankingErrorCode expected) {
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(call)
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(expected));
    }

    @Nested
    @DisplayName("topDrops — 진행 중 필터")
    class TopDrops {

        @Test
        @DisplayName("[T7-09] 1~3위 중 2위가 종료 드로우면 topDrops(2) 는 1위·3위를 순번 1·2 로 다시 매겨 반환한다")
        void skipsClosedAndRenumbers() {
            // Given
            givenDrop(1L, 30, OPEN_UNTIL);
            givenDrop(2L, 20, CLOSED_AT);
            givenDrop(3L, 10, OPEN_UNTIL);
            displayNameRepository.givenTitle(1L, "스니커즈");
            displayNameRepository.givenTitle(2L, "후디");
            displayNameRepository.givenTitle(3L, "캡");

            // When
            List<DropRankResult> result = service.topDrops(2);

            // Then
            assertThat(result).containsExactly(
                    new DropRankResult(1, 1L, "스니커즈", 30),
                    new DropRankResult(2, 3L, "캡", 10));
        }

        @Test
        @DisplayName("[T7-09 보강] closeAt 정각이면 종료로 본다, closeAt 미상(drop.created 미수신)은 진행 중으로 본다")
        void closeAtBoundaryAndUnknown() {
            // Given
            givenDrop(1L, 30, NOW);           // 정각 = 종료
            givenDrop(2L, 20, null);          // 미상 = 진행 중
            givenDrop(3L, 10, NOW.plusMillis(1));

            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(2L, 3L);
        }

        @Test
        @DisplayName("[T7-09 보강] 제목이 없는 드로우는 drop-{id} 로 표시한다")
        void titleFallback() {
            // Given
            givenDrop(5L, 3, OPEN_UNTIL);

            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).containsExactly(new DropRankResult(1, 5L, "drop-5", 3));
        }

        @Test
        @DisplayName("[T7-09 보강] 보드가 비어 있으면 빈 목록, removeDrops 를 호출하지 않는다")
        void emptyBoard() {
            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).isEmpty();
            assertThat(rankingRepository.removeDropsCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T7-10] 상위 60개가 모두 종료, 61번째가 진행 중이면 topDrops(1) 은 다음 배치까지 탐색해 61번째를 반환한다")
        void scansNextBatch() {
            // Given
            long openId = givenClosedThenOneOpen(60);

            // When
            List<DropRankResult> result = service.topDrops(1);

            // Then
            assertThat(result).containsExactly(new DropRankResult(1, openId, "drop-" + openId, 1));
            assertThat(rankingRepository.topCalls()).containsExactly(
                    new TopCall(RankingBoard.DROP_POPULAR, null, 0, RankingQueryService.SCAN_BATCH),
                    new TopCall(RankingBoard.DROP_POPULAR, null, 50, RankingQueryService.SCAN_BATCH));
        }

        @Test
        @DisplayName("[T7-29] 상위 499개가 종료, 500번째가 진행 중이면 topDrops(1) 은 500번째를 반환한다")
        void scanLimitInclusive() {
            // Given
            long openId = givenClosedThenOneOpen(499);
            assertThat(openId).isEqualTo(500L);

            // When
            List<DropRankResult> result = service.topDrops(1);

            // Then
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(500L);
        }

        @Test
        @DisplayName("[T7-29] 상위 500개가 종료, 501번째가 진행 중이면 빈 목록 — offset 500 은 조회하지 않는다 (SCAN_LIMIT)")
        void scanLimitExclusive() {
            // Given
            givenClosedThenOneOpen(500);

            // When
            List<DropRankResult> result = service.topDrops(1);

            // Then
            assertThat(result).isEmpty();
            assertThat(rankingRepository.topCalls())
                    .extracting(TopCall::offset)
                    .containsExactly(0L, 50L, 100L, 150L, 200L, 250L, 300L, 350L, 400L, 450L);
            assertThat(rankingRepository.topCalls()).allSatisfy(c -> assertThat(c.count()).isEqualTo(50));
        }

        @Test
        @DisplayName("[T7-29] 보드가 50개 미만이면 결과가 size 보다 적어도 top 을 1회만 호출한다 (조기 종료)")
        void earlyStopOnShortBatch() {
            // Given
            LongStream.rangeClosed(1, 30).forEach(id -> givenDrop(id, 100 - id, OPEN_UNTIL));

            // When
            List<DropRankResult> result = service.topDrops(100);

            // Then
            assertThat(result).hasSize(30);
            assertThat(rankingRepository.topCalls()).hasSize(1);
        }

        @Test
        @DisplayName("[T7-29 보강] size 만큼 채우면 다음 배치를 조회하지 않는다")
        void stopsWhenFilled() {
            // Given — 100개 모두 진행 중
            LongStream.rangeClosed(1, 100).forEach(id -> givenDrop(id, 1000 - id, OPEN_UNTIL));

            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).extracting(DropRankResult::rank).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
            assertThat(rankingRepository.topCalls()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("topDrops — 배치 사이 보드 변경·저장소 방어 (v1.3)")
    class ScanRobustness {

        @Test
        @DisplayName("[T7-29 보강] 두 번째 배치에 첫 배치 마지막 id(진행 중)가 다시 나와도 결과 dropId 는 유일하고 순번은 1부터 연속이다")
        void duplicateOpenAcrossBatches() {
            // Given — 1~49 종료, 50~60 진행 중. 두 번째 top 호출에 50 이 다시 끼어든다
            LongStream.rangeClosed(1, 49).forEach(id -> givenDrop(id, 10_000 - id, CLOSED_AT));
            LongStream.rangeClosed(50, 60).forEach(id -> givenDrop(id, 10_000 - id, OPEN_UNTIL));
            rankingRepository.injectOnTopCall(2, 50L, 10_000 - 50);

            // When
            List<DropRankResult> result = service.topDrops(5);

            // Then
            assertThat(rankingRepository.topCalls()).hasSize(2);
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(50L, 51L, 52L, 53L, 54L);
            assertThat(result).extracting(DropRankResult::rank).containsExactly(1L, 2L, 3L, 4L, 5L);
        }

        @Test
        @DisplayName("[T7-30 보강] 두 번째 배치에 첫 배치의 종료 id 가 다시 나와도 removeDrops 인자에 중복이 없다")
        void duplicateClosedAcrossBatches() {
            // Given — 1~50 종료, 51 진행 중. 두 번째 top 호출에 50 이 다시 끼어든다
            long openId = givenClosedThenOneOpen(50);
            rankingRepository.injectOnTopCall(2, 50L, 10_000 - 50);

            // When
            List<DropRankResult> result = service.topDrops(1);

            // Then
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(openId);
            assertThat(rankingRepository.removeDropsCalls()).hasSize(1);
            assertThat(rankingRepository.removeDropsCalls().getFirst())
                    .doesNotHaveDuplicates()
                    .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, 50).boxed().toList());
        }

        @Test
        @DisplayName("[T7-09 보강] DropWindowRepository 가 id 를 빠뜨려도 NPE 없이 진행 중으로 보고, 지우지 않는다")
        void missingWindowTreatedAsInProgress() {
            // Given
            givenDrop(1L, 30, null);
            givenDrop(2L, 20, OPEN_UNTIL);
            dropWindowRepository.omitFromFindAll(1L);

            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(1L, 2L);
            assertThat(rankingRepository.removeDropsCalls()).isEmpty();
        }
    }

    @Nested
    @DisplayName("topDrops — 종료 드로우 lazy 제거")
    class LazyRemoval {

        @Test
        @DisplayName("[T7-30] 종료 3개·closeAt 미상 1개·진행 중 1개 → 종료 3개만 removeDrops, 두 번째 조회에서는 제거 대상 없음")
        void removesOnlyClosed() {
            // Given
            givenDrop(1L, 50, CLOSED_AT);
            givenDrop(2L, 40, null);
            givenDrop(3L, 30, CLOSED_AT);
            givenDrop(4L, 20, OPEN_UNTIL);
            givenDrop(5L, 10, CLOSED_AT);

            // When
            List<DropRankResult> first = service.topDrops(10);

            // Then
            assertThat(first).extracting(DropRankResult::dropId).containsExactly(2L, 4L);
            assertThat(rankingRepository.removeDropsCalls()).hasSize(1);
            assertThat(rankingRepository.removeDropsCalls().getFirst()).containsExactlyInAnyOrder(1L, 3L, 5L);
            assertThat(rankingRepository.scores(RankingBoard.DROP_POPULAR, null)).containsOnlyKeys(2L, 4L);

            // When — 두 번째 조회
            List<DropRankResult> second = service.topDrops(10);

            // Then
            assertThat(second).isEqualTo(first);
            assertThat(rankingRepository.removeDropsCalls()).hasSize(1);
        }

        @Test
        @DisplayName("[T7-30 보강] 여러 배치에서 본 종료 드로우는 탐색이 끝난 뒤 한 번에 지운다 (탐색 중 지우면 offset 이 밀린다)")
        void removesOnceAfterScan() {
            // Given
            long openId = givenClosedThenOneOpen(60);

            // When
            List<DropRankResult> result = service.topDrops(1);

            // Then
            assertThat(result).extracting(DropRankResult::dropId).containsExactly(openId);
            assertThat(rankingRepository.removeDropsCalls()).hasSize(1);
            assertThat(rankingRepository.removeDropsCalls().getFirst())
                    .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, 60).boxed().toList());
        }

        @Test
        @DisplayName("[T7-30] removeDrops 가 실패해도 조회 결과를 돌려주고 WARN 을 남긴다")
        void removalFailureIgnored() {
            // Given
            givenDrop(1L, 30, OPEN_UNTIL);
            givenDrop(2L, 20, CLOSED_AT);
            rankingRepository.failRemoveDrops();

            // When
            List<DropRankResult> result = service.topDrops(10);

            // Then
            assertThat(result).containsExactly(new DropRankResult(1, 1L, "drop-1", 30));
            assertThat(rankingRepository.removeDropsCalls()).hasSize(1);
            assertThat(logs.messages(Level.WARN)).hasSize(1);
        }

        @Test
        @DisplayName("[T7-30 보강] 종료 드로우가 없으면 removeDrops 를 호출하지 않는다")
        void noClosedNoRemoval() {
            // Given
            givenDrop(1L, 30, OPEN_UNTIL);
            givenDrop(2L, 20, null);

            // When
            service.topDrops(10);

            // Then
            assertThat(rankingRepository.removeDropsCalls()).isEmpty();
        }
    }

    @Nested
    @DisplayName("weeklyUsers / allUsers")
    class Users {

        @Test
        @DisplayName("[T7-11] week 가 null 이면 현재 시각(Clock 고정)의 주차로 조회하고, 결과 week 도 그 값이다")
        void nullWeekUsesCurrentWeek() {
            // Given
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, THIS_WEEK, 7L, 5);
            displayNameRepository.givenNickname(7L, "원영");

            // When
            WeeklyRankingResult result = service.weeklyUsers(null, 10);

            // Then
            assertThat(result).isEqualTo(new WeeklyRankingResult("2026-W47",
                    List.of(new UserRankResult(1, 7L, "원영", 5))));
            assertThat(rankingRepository.topCalls())
                    .containsExactly(new TopCall(RankingBoard.USER_WEEKLY, THIS_WEEK, 0, 10));
        }

        @Test
        @DisplayName("[T7-11 보강] week null 의 현재 주차는 KST 기준 — UTC 일요일 15:00 (KST 월요일 00:00) 이면 다음 주차")
        void nullWeekUsesKst() {
            // Given
            RankingQueryService mondayKst = serviceAt(Instant.parse("2026-11-22T15:00:00Z"));

            // When
            WeeklyRankingResult result = mondayKst.weeklyUsers(null, 10);

            // Then
            assertThat(result.week()).isEqualTo("2026-W48");
            assertThat(result.items()).isEmpty();
        }

        @Test
        @DisplayName("[T7-11 보강] week 를 지정하면 그 주차 보드를 조회한다")
        void explicitWeek() {
            // Given
            WeekKey w46 = new WeekKey(2026, 46);
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, w46, 7L, 2);
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, THIS_WEEK, 8L, 9);

            // When
            WeeklyRankingResult result = service.weeklyUsers("2026-W46", 10);

            // Then
            assertThat(result).isEqualTo(new WeeklyRankingResult("2026-W46",
                    List.of(new UserRankResult(1, 7L, "user-7", 2))));
        }

        @ParameterizedTest(name = "week=\"{0}\"")
        @ValueSource(strings = {"abc", "", "2026-W99", "2026-W00", "2025-W53", "2026-W7", "2026-47"})
        @DisplayName("[T7-11] 형식이 틀리거나 존재하지 않는 주차는 INVALID_WEEK, 저장소를 조회하지 않는다")
        void invalidWeek(String week) {
            // When / Then
            assertErrorCode(() -> service.weeklyUsers(week, 10), RankingErrorCode.INVALID_WEEK);
            assertThat(rankingRepository.topCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T7-11 보강] allUsers 는 누적 보드를 점수 내림차순, 동점은 member 사전 역순, 닉네임 없으면 user-{id}")
        void allUsers() {
            // Given — 동점 5점: "9" > "10" (사전 역순)
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 10L, 5);
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 9L, 5);
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 3L, 8);
            displayNameRepository.givenNickname(10L, "열");

            // When
            List<UserRankResult> result = service.allUsers(10);

            // Then
            assertThat(result).containsExactly(
                    new UserRankResult(1, 3L, "user-3", 8),
                    new UserRankResult(2, 9L, "user-9", 5),
                    new UserRankResult(3, 10L, "열", 5));
            assertThat(rankingRepository.topCalls())
                    .containsExactly(new TopCall(RankingBoard.USER_ALL, null, 0, 10));
        }
    }

    @Nested
    @DisplayName("size 검증")
    class SizeValidation {

        static Stream<Arguments> calls() {
            return Stream.of(
                    Arguments.of("topDrops", (Consumer<Object[]>) a -> ((RankingQueryService) a[0]).topDrops((int) a[1])),
                    Arguments.of("weeklyUsers", (Consumer<Object[]>) a -> ((RankingQueryService) a[0]).weeklyUsers(null, (int) a[1])),
                    Arguments.of("allUsers", (Consumer<Object[]>) a -> ((RankingQueryService) a[0]).allUsers((int) a[1])));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("calls")
        @DisplayName("[T7-11] size 0, 101 은 INVALID_SIZE, 저장소를 조회하지 않는다")
        void outOfRange(String name, Consumer<Object[]> call) {
            for (int size : new int[] {0, 101, -1}) {
                // When / Then
                assertErrorCode(() -> call.accept(new Object[] {service, size}), RankingErrorCode.INVALID_SIZE);
            }
            assertThat(rankingRepository.topCalls()).isEmpty();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("calls")
        @DisplayName("[T7-11 보강] size 1, 100 은 허용한다 (경계)")
        void boundaries(String name, Consumer<Object[]> call) {
            // When / Then
            assertThatCode(() -> call.accept(new Object[] {service, 1})).doesNotThrowAnyException();
            assertThatCode(() -> call.accept(new Object[] {service, 100})).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("[T7-11 보강] size 와 week 가 모두 틀리면 저장소 장애여도 검증 오류가 먼저다 (size → INVALID_SIZE)")
        void validationBeforeStore() {
            // Given
            rankingRepository.failTop();

            // When / Then
            assertErrorCode(() -> service.weeklyUsers("abc", 0), RankingErrorCode.INVALID_SIZE);
            assertErrorCode(() -> service.weeklyUsers("abc", 10), RankingErrorCode.INVALID_WEEK);
            assertErrorCode(() -> service.topDrops(0), RankingErrorCode.INVALID_SIZE);
        }
    }

    @Nested
    @DisplayName("myRank")
    class MyRank {

        @Test
        @DisplayName("[T7-12] 랭킹에 없는 사용자는 rank null, score 0, week 는 이번 주")
        void notRanked() {
            // When
            MyRankResult result = service.myRank(7L);

            // Then
            assertThat(result).isEqualTo(new MyRankResult("2026-W47", null, 0, null, 0));
        }

        @Test
        @DisplayName("[T7-12 보강] 누적에만 있고 이번 주에는 없으면 weekly 만 null/0")
        void onlyAllTime() {
            // Given
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 3L, 9);
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 7L, 4);
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, new WeekKey(2026, 46), 7L, 4);

            // When
            MyRankResult result = service.myRank(7L);

            // Then
            assertThat(result).isEqualTo(new MyRankResult("2026-W47", null, 0, 2L, 4));
        }

        @Test
        @DisplayName("[T7-12 보강] 이번 주·누적 모두 있으면 각 순번과 점수를 반환한다")
        void ranked() {
            // Given
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, THIS_WEEK, 7L, 3);
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, THIS_WEEK, 8L, 5);
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 7L, 9);

            // When
            MyRankResult result = service.myRank(7L);

            // Then
            assertThat(result).isEqualTo(new MyRankResult("2026-W47", 2L, 3, 1L, 9));
        }
    }

    @Nested
    @DisplayName("저장소 장애 → TEMPORARILY_FAILED")
    class StoreFailure {

        private void assertTemporarilyFailed(ThrowingCallable call) {
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(call)
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(RankingErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isInstanceOf(RankingStoreUnavailableException.class);
                    });
        }

        @Test
        @DisplayName("[T7-27] top 실패 → topDrops·weeklyUsers·allUsers 모두 TEMPORARILY_FAILED(원인 RankingStoreUnavailableException)")
        void topFailure() {
            // Given
            rankingRepository.failTop();

            // When / Then
            assertTemporarilyFailed(() -> service.topDrops(10));
            assertTemporarilyFailed(() -> service.weeklyUsers(null, 10));
            assertTemporarilyFailed(() -> service.weeklyUsers("2026-W46", 10));
            assertTemporarilyFailed(() -> service.allUsers(10));
        }

        @Test
        @DisplayName("[T7-27] rankOf 실패 → myRank TEMPORARILY_FAILED")
        void rankOfFailure() {
            // Given
            rankingRepository.failRankOf();

            // When / Then
            assertTemporarilyFailed(() -> service.myRank(7L));
        }

        @Test
        @DisplayName("[T7-27 보강] closeAt 조회(findAll) 실패 → topDrops TEMPORARILY_FAILED, removeDrops 호출 없음")
        void dropWindowFailure() {
            // Given
            givenDrop(1L, 10, CLOSED_AT);
            dropWindowRepository.failFindAll();

            // When / Then
            assertTemporarilyFailed(() -> service.topDrops(10));
            assertThat(rankingRepository.removeDropsCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T7-27 보강] 제목 조회 실패 → topDrops TEMPORARILY_FAILED")
        void titleFailure() {
            // Given
            givenDrop(1L, 10, OPEN_UNTIL);
            displayNameRepository.failFindDropTitles();

            // When / Then
            assertTemporarilyFailed(() -> service.topDrops(10));
        }

        @Test
        @DisplayName("[T7-27 보강] 닉네임 조회 실패 → weeklyUsers·allUsers TEMPORARILY_FAILED")
        void nicknameFailure() {
            // Given
            rankingRepository.givenScore(RankingBoard.USER_WEEKLY, THIS_WEEK, 7L, 1);
            rankingRepository.givenScore(RankingBoard.USER_ALL, null, 7L, 1);
            displayNameRepository.failFindNicknames();

            // When / Then
            assertTemporarilyFailed(() -> service.weeklyUsers(null, 10));
            assertTemporarilyFailed(() -> service.allUsers(10));
        }
    }
}
