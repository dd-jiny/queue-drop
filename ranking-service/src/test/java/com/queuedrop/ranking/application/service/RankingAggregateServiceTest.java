package com.queuedrop.ranking.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import ch.qos.logback.classic.Level;
import com.queuedrop.ranking.application.dto.DropInfoCommand;
import com.queuedrop.ranking.application.dto.RankingRecordCommand;
import com.queuedrop.ranking.application.dto.UserInfoCommand;
import com.queuedrop.ranking.application.fake.InMemoryDisplayNameRepository;
import com.queuedrop.ranking.application.fake.InMemoryDropWindowRepository;
import com.queuedrop.ranking.application.fake.InMemoryRankingRepository;
import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** RankingAggregateService 단위 (Fake 저장소). 쓰기 경로: 집계·표시 정보 저장 (단계 문서 3.2) */
class RankingAggregateServiceTest {

    /** 2026-W46 의 마지막 순간 (지난주 일요일 23:59 KST). 이번 주는 2026-W47 (11/16~11/22) */
    private static final Instant LAST_SUNDAY_2359_KST = OffsetDateTime.parse("2026-11-15T23:59:00+09:00").toInstant();
    private static final Instant THIS_WEEK = OffsetDateTime.parse("2026-11-18T12:00:00+09:00").toInstant();
    private static final WeekKey W46 = new WeekKey(2026, 46);
    private static final WeekKey W47 = new WeekKey(2026, 47);
    private static final Instant CLOSE_AT = Instant.parse("2026-11-20T10:00:00Z");

    private InMemoryRankingRepository rankingRepository;
    private InMemoryDisplayNameRepository displayNameRepository;
    private InMemoryDropWindowRepository dropWindowRepository;
    private RankingAggregateService service;
    private LogCapture logs;

    @BeforeEach
    void setUp() {
        rankingRepository = new InMemoryRankingRepository();
        displayNameRepository = new InMemoryDisplayNameRepository();
        dropWindowRepository = new InMemoryDropWindowRepository();
        service = new RankingAggregateService(rankingRepository, displayNameRepository, dropWindowRepository);
        logs = new LogCapture(RankingAggregateService.class);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private void assertNoStoreCalls() {
        assertThat(rankingRepository.incrementCalls()).isEmpty();
        assertThat(displayNameRepository.saveDropTitleCalls()).isEmpty();
        assertThat(displayNameRepository.saveNicknameCalls()).isEmpty();
        assertThat(dropWindowRepository.saveCalls()).isEmpty();
    }

    @Nested
    @DisplayName("recordEntry")
    class RecordEntry {

        @Test
        @DisplayName("[T7-08] appliedAt 이 지난주 일요일 23:59 KST 면 (처리 시각이 이번 주여도) 지난주 키에 집계한다")
        void usesAppliedAtWeek() {
            // Given — 처리 시각(THIS_WEEK)과 무관하게 이벤트 시각으로 주차를 정해야 한다
            assertThat(WeekKey.of(THIS_WEEK)).isEqualTo(W47);
            RankingRecordCommand cmd = new RankingRecordCommand(3L, 7L, LAST_SUNDAY_2359_KST);

            // When
            boolean recorded = service.recordEntry(cmd);

            // Then
            assertThat(recorded).isTrue();
            assertThat(rankingRepository.incrementCalls())
                    .containsExactly(new InMemoryRankingRepository.IncrementCall(3L, 7L, W46));
            assertThat(rankingRepository.scores(RankingBoard.USER_WEEKLY, W46)).isEqualTo(Map.of(7L, 1L));
            assertThat(rankingRepository.scores(RankingBoard.USER_WEEKLY, W47)).isEmpty();
            assertThat(rankingRepository.scores(RankingBoard.USER_ALL, null)).isEqualTo(Map.of(7L, 1L));
            assertThat(rankingRepository.scores(RankingBoard.DROP_POPULAR, null)).isEqualTo(Map.of(3L, 1L));
        }

        @Test
        @DisplayName("[T7-08 보강] 같은 (dropId, userId) 두 번째 호출은 false, 점수는 1 그대로")
        void duplicateReturnsFalse() {
            // Given
            RankingRecordCommand cmd = new RankingRecordCommand(3L, 7L, THIS_WEEK);
            service.recordEntry(cmd);

            // When
            boolean second = service.recordEntry(cmd);

            // Then
            assertThat(second).isFalse();
            assertThat(rankingRepository.scores(RankingBoard.USER_ALL, null)).isEqualTo(Map.of(7L, 1L));
        }

        @Test
        @DisplayName("[T7-28 보강] Redis 장애(RankingStoreUnavailableException)는 잡지 않고 전파한다 — 리스너 재시도 대상")
        void storeFailurePropagates() {
            // Given
            rankingRepository.failIncrement();

            // When / Then
            assertThatExceptionOfType(RankingStoreUnavailableException.class)
                    .isThrownBy(() -> service.recordEntry(new RankingRecordCommand(3L, 7L, THIS_WEEK)))
                    .isSameAs(rankingRepository.lastFailure());
        }

        static Stream<Arguments> invalidEntries() {
            Instant at = Instant.parse("2026-11-18T03:00:00Z");
            return Stream.of(
                    Arguments.of("dropId null", new RankingRecordCommand(null, 7L, at)),
                    Arguments.of("dropId 0", new RankingRecordCommand(0L, 7L, at)),
                    Arguments.of("dropId 음수", new RankingRecordCommand(-1L, 7L, at)),
                    Arguments.of("userId null", new RankingRecordCommand(3L, null, at)),
                    Arguments.of("userId 0", new RankingRecordCommand(3L, 0L, at)),
                    Arguments.of("appliedAt null", new RankingRecordCommand(3L, 7L, null)));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("invalidEntries")
        @DisplayName("[T7-28] 필드 오류인 entry.created 는 예외 없이 false, WARN, 저장소 호출 0회")
        void invalidSkipped(String label, RankingRecordCommand cmd) {
            // When
            boolean recorded = service.recordEntry(cmd);

            // Then
            assertThat(recorded).isFalse();
            assertNoStoreCalls();
            assertThat(logs.messages(Level.WARN)).hasSize(1);
        }
    }

    @Nested
    @DisplayName("registerDrop")
    class RegisterDrop {

        @Test
        @DisplayName("[T7-28 보강] 정상 drop.created 는 제목과 closeAt 을 모두 저장한다")
        void savesTitleAndCloseAt() {
            // When
            service.registerDrop(new DropInfoCommand(3L, "한정판 스니커즈", CLOSE_AT));

            // Then
            assertThat(displayNameRepository.titles()).isEqualTo(Map.of(3L, "한정판 스니커즈"));
            assertThat(dropWindowRepository.closeAts()).isEqualTo(Map.of(3L, CLOSE_AT));
            assertThat(logs.messages(Level.WARN)).isEmpty();
        }

        static Stream<Arguments> invalidDrops() {
            Instant closeAt = Instant.parse("2026-11-20T10:00:00Z");
            return Stream.of(
                    Arguments.of("dropId null", new DropInfoCommand(null, "제목", closeAt)),
                    Arguments.of("dropId 0", new DropInfoCommand(0L, "제목", closeAt)),
                    Arguments.of("title null", new DropInfoCommand(3L, null, closeAt)),
                    Arguments.of("title 빈 문자열", new DropInfoCommand(3L, "", closeAt)),
                    Arguments.of("title 공백", new DropInfoCommand(3L, "   ", closeAt)),
                    Arguments.of("closeAt 만 null", new DropInfoCommand(3L, "제목", null)));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("invalidDrops")
        @DisplayName("[T7-28] 필드 오류인 drop.created 는 예외 없이 반환, WARN, 저장 0회 (closeAt 만 null 이어도 제목을 저장하지 않음)")
        void invalidSkipped(String label, DropInfoCommand cmd) {
            // When
            service.registerDrop(cmd);

            // Then
            assertNoStoreCalls();
            assertThat(displayNameRepository.titles()).isEmpty();
            assertThat(dropWindowRepository.closeAts()).isEmpty();
            assertThat(logs.messages(Level.WARN)).hasSize(1);
        }

        @Test
        @DisplayName("[T7-28 보강] 저장 중 Redis 장애는 전파한다")
        void storeFailurePropagates() {
            // Given
            displayNameRepository.failSave();

            // When / Then
            assertThatExceptionOfType(RankingStoreUnavailableException.class)
                    .isThrownBy(() -> service.registerDrop(new DropInfoCommand(3L, "제목", CLOSE_AT)));
        }

        @Test
        @DisplayName("[T7-28 보강] 두 번째 쓰기(closeAt 저장) 중 Redis 장애도 전파한다 — 재시도가 둘 다 다시 쓴다 (HSET 멱등)")
        void secondWriteFailurePropagates() {
            // Given
            dropWindowRepository.failSave();

            // When / Then
            assertThatExceptionOfType(RankingStoreUnavailableException.class)
                    .isThrownBy(() -> service.registerDrop(new DropInfoCommand(3L, "제목", CLOSE_AT)));
            assertThat(dropWindowRepository.saveCalls())
                    .containsExactly(new InMemoryDropWindowRepository.SaveCall(3L, CLOSE_AT));
        }
    }

    @Nested
    @DisplayName("registerUser")
    class RegisterUser {

        @Test
        @DisplayName("[T7-28 보강] 정상 user.registered 는 닉네임을 저장한다")
        void savesNickname() {
            // When
            service.registerUser(new UserInfoCommand(7L, "원영"));

            // Then
            assertThat(displayNameRepository.nicknames()).isEqualTo(Map.of(7L, "원영"));
            assertThat(logs.messages(Level.WARN)).isEmpty();
        }

        static Stream<Arguments> invalidUsers() {
            return Stream.of(
                    Arguments.of("userId null", new UserInfoCommand(null, "닉")),
                    Arguments.of("userId 음수", new UserInfoCommand(-5L, "닉")),
                    Arguments.of("nickname null", new UserInfoCommand(7L, null)),
                    Arguments.of("nickname 공백", new UserInfoCommand(7L, " ")));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("invalidUsers")
        @DisplayName("[T7-28] 필드 오류인 user.registered 는 예외 없이 반환, WARN, 저장 0회")
        void invalidSkipped(String label, UserInfoCommand cmd) {
            // When
            service.registerUser(cmd);

            // Then
            assertNoStoreCalls();
            assertThat(logs.messages(Level.WARN)).hasSize(1);
        }
    }
}
