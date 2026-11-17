package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.entry.application.config.DrawProperties;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.dto.ManualDrawCommand;
import com.queuedrop.entry.application.dto.Requester;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.application.service.fake.FakeApplicantCounter;
import com.queuedrop.entry.application.service.fake.FakeDrawEventPublisher;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.application.service.fake.FakeEntryGate.ExpireCall;
import com.queuedrop.entry.application.service.fake.FixedSeedGenerator;
import com.queuedrop.entry.application.service.fake.InMemoryDrawResultRepository;
import com.queuedrop.entry.application.service.fake.InMemoryDropSnapshotRepository;
import com.queuedrop.entry.application.service.fake.InMemoryEntryRepository;
import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import com.queuedrop.entry.domain.service.Drawer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class DrawServiceTest {

    private static final long DROP_ID = 7L;
    private static final long OTHER_DROP_ID = 8L;
    private static final long SEED = 42L;
    private static final long ADMIN_ID = 1L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-17T12:00:00Z");
    /** 마감 직후 (장기화 기준 5분 이내) */
    private static final Instant NOW = Instant.parse("2026-11-17T12:00:05Z");
    private static final Duration WARN_THRESHOLD = Duration.ofMinutes(5);
    private static final Duration KEY_TTL = Duration.ofDays(1);

    private RacingSnapshotRepository snapshots;
    private InMemoryEntryRepository entries;
    private InMemoryDrawResultRepository results;
    private FakeApplicantCounter applicantCounter;
    private FakeEntryGate gate;
    private FakeDrawEventPublisher publisher;
    private FixedSeedGenerator seeds;
    private DrawService service;

    @BeforeEach
    void setUp() {
        snapshots = new RacingSnapshotRepository();
        entries = new InMemoryEntryRepository();
        results = new InMemoryDrawResultRepository();
        applicantCounter = new FakeApplicantCounter();
        gate = new FakeEntryGate();
        publisher = new FakeDrawEventPublisher();
        seeds = new FixedSeedGenerator(SEED);
        DrawTransaction drawTransaction = new DrawTransaction(snapshots, entries, results, seeds);
        service = new DrawService(snapshots, entries, results, applicantCounter, gate, drawTransaction, publisher,
                new DrawProperties(WARN_THRESHOLD, KEY_TTL));
    }

    // ---------------------------------------------------------------- 준비 도우미

    private void givenOpenDrop(long dropId, Instant closeAt) {
        snapshots.saveIfAbsent(DropSnapshot.register(dropId, 100, 1, OPEN_AT, closeAt));
    }

    /** CLOSING 드로우 + DB 응모 applicantCount 건 (userId = dropId*1000 + seq). entry_seq 오름차순 userId 를 돌려준다 */
    private List<Long> givenClosingDrop(long dropId, int winnerCount, int applicantCount) {
        snapshots.saveIfAbsent(DropSnapshot.register(dropId, Math.max(winnerCount, applicantCount), winnerCount,
                OPEN_AT, CLOSE_AT));
        snapshots.forceStatus(dropId, SnapshotStatus.CLOSING);
        List<Long> userIds = new ArrayList<>();
        for (int seq = 1; seq <= applicantCount; seq++) {
            long userId = dropId * 1000 + seq;
            entries.saveIfAbsent(Entry.apply(dropId, userId, seq, OPEN_AT));
            userIds.add(userId);
        }
        return userIds;
    }

    private SnapshotStatus statusOf(long dropId) {
        return snapshots.findById(dropId).orElseThrow().getStatus();
    }

    private List<EntryStatus> entryStatuses(long dropId, List<Long> userIds) {
        return userIds.stream()
                .map(userId -> entries.findByDropIdAndUserId(dropId, userId).orElseThrow().getStatus())
                .toList();
    }

    private List<Long> usersWithStatus(long dropId, List<Long> userIds, EntryStatus status) {
        return userIds.stream()
                .filter(userId -> entries.findByDropIdAndUserId(dropId, userId).orElseThrow().getStatus() == status)
                .toList();
    }

    /** WARN 레벨이면서 모든 조각을 포함한 출력 줄 수 */
    private static long warnLines(CapturedOutput output, String... fragments) {
        return output.getAll().lines()
                .filter(line -> line.contains("WARN"))
                .filter(line -> Arrays.stream(fragments).allMatch(line::contains))
                .count();
    }

    /** ERROR 레벨이면서 모든 조각을 포함한 출력 줄 수 */
    private static long errorLines(CapturedOutput output, String... fragments) {
        return output.getAll().lines()
                .filter(line -> line.contains("ERROR"))
                .filter(line -> Arrays.stream(fragments).allMatch(line::contains))
                .count();
    }

    private static ManualDrawCommand manual(String role, long dropId) {
        return new ManualDrawCommand(new Requester(ADMIN_ID, role), dropId);
    }

    private static void assertErrorCode(ApplicationException e, ErrorCode expected) {
        assertThat(e.errorCode()).isEqualTo(expected);
    }

    // ---------------------------------------------------------------- T5-09 마감 잠금

    @Nested
    @DisplayName("closeDueDrops")
    class CloseDueDrops {

        @Test
        @DisplayName("[T5-09] 마감 지난 OPEN 드로우는 CLOSING 으로 바뀌고 Gate 에 close 가 기록된다")
        void closesDueDrop() {
            // Given
            givenOpenDrop(DROP_ID, CLOSE_AT);

            // When
            service.closeDueDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.closeCalls()).containsExactly(DROP_ID);
            assertThat(gate.isClosed(DROP_ID)).isTrue();
        }

        @Test
        @DisplayName("[T5-09] closeAt == now 인 드로우도 마감된다 (closeAt <= now 경계)")
        void closesAtExactCloseAt() {
            // Given
            givenOpenDrop(DROP_ID, CLOSE_AT);

            // When
            service.closeDueDrops(CLOSE_AT);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.closeCalls()).containsExactly(DROP_ID);
        }

        @Test
        @DisplayName("[T5-09] 마감 전(closeAt > now) 드로우는 OPEN 그대로, close 호출 없음")
        void notDueStaysOpen() {
            // Given
            givenOpenDrop(DROP_ID, NOW.plusSeconds(1));

            // When
            service.closeDueDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.OPEN);
            assertThat(gate.closeCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T5-09] 이미 CLOSING 인 드로우는 다시 close 하지 않는다")
        void closingNotClosedAgain() {
            // Given
            givenClosingDrop(DROP_ID, 1, 0);

            // When
            service.closeDueDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.closeCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T5-09] 첫 드로우의 gate.close 가 GateUnavailableException 이어도 예외 없이 다음 드로우까지 마감한다 (재시도 없음)")
        void gateFailureDoesNotStopOthers() {
            // Given
            givenOpenDrop(DROP_ID, CLOSE_AT);
            givenOpenDrop(OTHER_DROP_ID, CLOSE_AT);
            gate.failCloseNext();

            // When
            assertThatCode(() -> service.closeDueDrops(NOW)).doesNotThrowAnyException();

            // Then: 둘 다 DB 는 CLOSING. Redis 는 두 번째만 CLOSED (첫 번째는 Lua 의 closeAt 검사가 응모 차단)
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(statusOf(OTHER_DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.closeCalls()).containsExactly(DROP_ID, OTHER_DROP_ID);
            assertThat(gate.isClosed(OTHER_DROP_ID)).isTrue();

            // When: 다음 주기
            service.closeDueDrops(NOW.plusSeconds(5));

            // Then: 이미 CLOSING 이라 다시 close 하지 않는다
            assertThat(gate.closeCalls()).containsExactly(DROP_ID, OTHER_DROP_ID);
        }

        @Test
        @DisplayName("[T5-09 보강] gate.close 실패는 dropId 를 담은 WARN 1건으로 남고, 성공한 드로우에는 WARN 이 없다")
        void gateFailureLogsWarn(CapturedOutput output) {
            // Given
            givenOpenDrop(DROP_ID, CLOSE_AT);
            givenOpenDrop(OTHER_DROP_ID, CLOSE_AT);
            gate.failCloseNext();

            // When
            service.closeDueDrops(NOW);

            // Then
            assertThat(warnLines(output, "entry gate close failed", "dropId=" + DROP_ID)).isEqualTo(1);
            assertThat(warnLines(output, "entry gate close failed", "dropId=" + OTHER_DROP_ID)).isZero();
        }

        @Test
        @DisplayName("[T5-09 보강] 첫 드로우 markClosingIfOpen 이 RuntimeException 이어도 예외 없이 다음 드로우는 CLOSING + gate.close, 실패 드로우는 ERROR 1건 후 다음 주기에 마감된다")
        void markClosingFailureIsolatedPerDrop(CapturedOutput output) {
            // Given: 첫 드로우의 조건부 UPDATE 만 1회 실패 (DB 일시 장애)
            givenOpenDrop(DROP_ID, CLOSE_AT);
            givenOpenDrop(OTHER_DROP_ID, CLOSE_AT);
            snapshots.failMarkClosingOnceFor(DROP_ID, new IllegalStateException("db down"));

            // When
            assertThatCode(() -> service.closeDueDrops(NOW)).doesNotThrowAnyException();

            // Then: 두 번째 드로우는 정상 마감, 첫 드로우는 OPEN 유지·close 없음
            assertThat(statusOf(OTHER_DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.isClosed(OTHER_DROP_ID)).isTrue();
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.OPEN);
            assertThat(gate.closeCalls()).containsExactly(OTHER_DROP_ID);
            assertThat(errorLines(output, "drop close failed", "dropId=" + DROP_ID)).isEqualTo(1);
            assertThat(errorLines(output, "drop close failed", "dropId=" + OTHER_DROP_ID)).isZero();

            // When: 다음 주기 (DB 복구)
            service.closeDueDrops(NOW.plusSeconds(5));

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(gate.closeCalls()).containsExactly(OTHER_DROP_ID, DROP_ID);
        }
    }

    // ---------------------------------------------------------------- T5-10 ~ T5-13, T5-30 자동 추첨

    @Nested
    @DisplayName("drawReadyDrops")
    class DrawReadyDrops {

        @Test
        @DisplayName("[T5-10] CLOSING, Redis 1000 / DB 987 이면 추첨하지 않는다 (상태·응모·기록·발행·키 만료 모두 그대로)")
        void countMismatchWaits() {
            // Given
            List<Long> userIds = givenClosingDrop(DROP_ID, 10, 987);
            applicantCounter.set(DROP_ID, 1000);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(entryStatuses(DROP_ID, userIds)).containsOnly(EntryStatus.APPLIED);
            assertThat(results.count()).isZero();
            assertThat(publisher.attempted()).isEmpty();
            assertThat(seeds.calls()).isZero();
            assertThat(gate.expireCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T5-10] DB 가 Redis 보다 많아도(Redis 5 / DB 6) 불일치로 보고 추첨하지 않는다 (!= 비교)")
        void dbGreaterThanRedisAlsoWaits() {
            // Given
            givenClosingDrop(DROP_ID, 2, 6);
            applicantCounter.set(DROP_ID, 5);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(results.count()).isZero();
            assertThat(publisher.attempted()).isEmpty();
        }

        @Test
        @DisplayName("[T5-11] 건수 일치(5/5), K=2, 고정 시드 → DRAWN, 기록 1건(seed·건수·published=true), 발행 1회, WON 은 Drawer 결과와 같다")
        void drawsWhenCountsMatch() {
            // Given
            List<Long> userIds = givenClosingDrop(DROP_ID, 2, 5);
            applicantCounter.set(DROP_ID, 5);
            Set<Long> expectedWinners = new Drawer().draw(userIds, 2, SEED);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            DrawResult result = results.findByDropId(DROP_ID).orElseThrow();
            assertThat(result.getSeed()).isEqualTo(SEED);
            assertThat(result.getApplicantCount()).isEqualTo(5);
            assertThat(result.getWinnerCount()).isEqualTo(2);
            assertThat(result.getDrawnAt()).isEqualTo(NOW);
            assertThat(result.isPublished()).isTrue();
            assertThat(seeds.calls()).isEqualTo(1);
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 5, 2, NOW));
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.WON)).containsExactlyInAnyOrderElementsOf(expectedWinners);
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.LOST)).hasSize(3);
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.APPLIED)).isEmpty();
        }

        @Test
        @DisplayName("[T5-11] 추첨 후 다시 호출해도 재추첨·재발행하지 않는다 (DRAWN 은 대상 아님)")
        void secondRunDoesNothing() {
            // Given
            givenClosingDrop(DROP_ID, 2, 5);
            applicantCounter.set(DROP_ID, 5);
            service.drawReadyDrops(NOW);

            // When
            service.drawReadyDrops(NOW.plusSeconds(5));

            // Then
            assertThat(seeds.calls()).isEqualTo(1);
            assertThat(publisher.attempted()).hasSize(1);
            assertThat(results.count()).isEqualTo(1);
            assertThat(gate.expireCalls()).hasSize(1);
        }

        @Test
        @DisplayName("[T5-11] 응모자 3명, K=5 → 3명 전원 WON, winnerCount=3")
        void fewerApplicantsThanWinners() {
            // Given
            List<Long> userIds = givenClosingDrop(DROP_ID, 5, 3);
            applicantCounter.set(DROP_ID, 3);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(entryStatuses(DROP_ID, userIds)).containsOnly(EntryStatus.WON);
            assertThat(results.findByDropId(DROP_ID).orElseThrow().getWinnerCount()).isEqualTo(3);
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 3, 3, NOW));
        }

        @Test
        @DisplayName("[T5-11] 응모 0건(Redis 0 / DB 0)도 추첨 완료 처리한다 — CLOSING 에 묶이지 않음 (applicantCount=0, winnerCount=0, 발행 1회)")
        void zeroApplicantsDrawn() {
            // Given: Redis 키 자체가 없어 count=0
            givenClosingDrop(DROP_ID, 2, 0);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            DrawResult result = results.findByDropId(DROP_ID).orElseThrow();
            assertThat(result.getApplicantCount()).isZero();
            assertThat(result.getWinnerCount()).isZero();
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 0, 0, NOW));
        }

        @Test
        @DisplayName("[T5-11] ApplicantCounter 장애(GateUnavailableException) 드로우는 건너뛰고(추첨 안 함) 다음 드로우는 추첨한다")
        void counterFailureSkipsOnlyThatDrop() {
            // Given: 첫 번째 count 만 실패
            List<Long> firstUsers = givenClosingDrop(DROP_ID, 1, 2);
            givenClosingDrop(OTHER_DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 2);
            applicantCounter.set(OTHER_DROP_ID, 2);
            applicantCounter.failNext();

            // When
            assertThatCode(() -> service.drawReadyDrops(NOW)).doesNotThrowAnyException();

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(entryStatuses(DROP_ID, firstUsers)).containsOnly(EntryStatus.APPLIED);
            assertThat(results.findByDropId(DROP_ID)).isEmpty();
            assertThat(statusOf(OTHER_DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).extracting(DrawCompletedPayload::dropId).containsExactly(OTHER_DROP_ID);
            assertThat(seeds.calls()).isEqualTo(1);

            // When: 다음 주기 (Redis 복구)
            service.drawReadyDrops(NOW.plusSeconds(5));

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).extracting(DrawCompletedPayload::dropId)
                    .containsExactly(OTHER_DROP_ID, DROP_ID);
        }

        @Test
        @DisplayName("[T5-11 보강] 첫 CLOSING 드로우의 DB 건수 조회가 RuntimeException 이어도 예외 없이 다음 드로우는 DRAWN·발행, 실패 드로우는 ERROR 1건 후 다음 주기에 추첨된다")
        void drawFailureIsolatedPerDrop(CapturedOutput output) {
            // Given: 첫 드로우의 countByDropId 만 1회 실패 (DB 일시 장애)
            List<Long> firstUsers = givenClosingDrop(DROP_ID, 1, 2);
            givenClosingDrop(OTHER_DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 2);
            applicantCounter.set(OTHER_DROP_ID, 2);
            entries.failCountOnceFor(DROP_ID, new IllegalStateException("db down"));

            // When
            assertThatCode(() -> service.drawReadyDrops(NOW)).doesNotThrowAnyException();

            // Then: 두 번째 드로우만 추첨·발행
            assertThat(statusOf(OTHER_DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).extracting(DrawCompletedPayload::dropId).containsExactly(OTHER_DROP_ID);
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(entryStatuses(DROP_ID, firstUsers)).containsOnly(EntryStatus.APPLIED);
            assertThat(results.findByDropId(DROP_ID)).isEmpty();
            assertThat(errorLines(output, "draw failed", "dropId=" + DROP_ID)).isEqualTo(1);
            assertThat(errorLines(output, "draw failed", "dropId=" + OTHER_DROP_ID)).isZero();

            // When: 다음 주기 (DB 복구)
            service.drawReadyDrops(NOW.plusSeconds(5));

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).extracting(DrawCompletedPayload::dropId)
                    .containsExactly(OTHER_DROP_ID, DROP_ID);
            assertThat(seeds.calls()).isEqualTo(2);
        }

        @Test
        @DisplayName("[T5-12 보강] 발행 성공 후 markPublished 가 RuntimeException → 예외 없이 published=false 유지·키 만료 진행, WARN 은 'published but mark failed' 1건뿐 (발행 실패 WARN 없음)")
        void markPublishedFailureLoggedSeparately(CapturedOutput output) {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 2);
            results.failMarkPublishedNext(new IllegalStateException("db down"));

            // When
            assertThatCode(() -> service.drawReadyDrops(NOW)).doesNotThrowAnyException();

            // Then: 발행은 됐고 표시만 실패 → 다음 주기 재발행 대상 (소비자 멱등)
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 2, 1, NOW));
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isFalse();
            assertThat(gate.expireCalls()).containsExactly(new ExpireCall(DROP_ID, KEY_TTL));
            assertThat(warnLines(output, "draw.completed published but mark failed", "dropId=" + DROP_ID)).isEqualTo(1);
            assertThat(warnLines(output, "draw.completed publish failed")).isZero();
        }

        @Test
        @DisplayName("[T5-12] 발행 실패 → 예외 없이 결과 저장·DRAWN 유지, published=false. 복구 후 republishUnpublished 에서 같은 payload 1회 발행, published=true")
        void publishFailureThenRepublish() {
            // Given
            List<Long> userIds = givenClosingDrop(DROP_ID, 2, 5);
            applicantCounter.set(DROP_ID, 5);
            publisher.failAlways();

            // When
            assertThatCode(() -> service.drawReadyDrops(NOW)).doesNotThrowAnyException();

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.APPLIED)).isEmpty();
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isFalse();
            assertThat(publisher.attempted()).hasSize(1);
            assertThat(publisher.published()).isEmpty();
            assertThat(gate.expireCalls()).as("발행 실패여도 키 만료는 진행").containsExactly(new ExpireCall(DROP_ID, KEY_TTL));

            // When: 브로커 실패 지속 중 재발행 → 여전히 미발행, 예외 없음
            assertThatCode(() -> service.republishUnpublished()).doesNotThrowAnyException();
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isFalse();

            // When: 복구 후 재발행
            publisher.recover();
            service.republishUnpublished();

            // Then
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 5, 2, NOW));
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isTrue();
            assertThat(seeds.calls()).as("재발행은 재추첨이 아니다").isEqualTo(1);

            // When: 한 번 더
            service.republishUnpublished();

            // Then: 이미 발행된 건은 다시 보내지 않는다
            assertThat(publisher.published()).hasSize(1);
        }

        @Test
        @DisplayName("[T5-12] republishUnpublished 는 한 건 발행이 실패해도 나머지 미발행 건을 계속 발행한다")
        void republishContinuesAfterOneFailure() {
            // Given: 미발행 기록 2건
            results.save(DrawResult.of(DROP_ID, 1L, 3, 1, NOW));
            results.save(DrawResult.of(OTHER_DROP_ID, 2L, 4, 2, NOW));
            publisher.failNext();

            // When
            assertThatCode(() -> service.republishUnpublished()).doesNotThrowAnyException();

            // Then
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(OTHER_DROP_ID, 4, 2, NOW));
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isFalse();
            assertThat(results.findByDropId(OTHER_DROP_ID).orElseThrow().isPublished()).isTrue();
        }

        @Test
        @DisplayName("[T5-13] CLOSING 이 기준(5분) 초과(closeAt+5m+1s), 건수 불일치 → WARN 1건 (dropId, redis·db 수), 추첨 안 함")
        void warnsWhenClosingTooLong(CapturedOutput output) {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 3);

            // When
            service.drawReadyDrops(CLOSE_AT.plus(WARN_THRESHOLD).plusSeconds(1));

            // Then
            assertThat(warnLines(output, "dropId=" + DROP_ID, "redis=3", "db=2")).isEqualTo(1);
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
            assertThat(results.count()).isZero();
        }

        @Test
        @DisplayName("[T5-13] 기준 시각 정각(closeAt+5m)에는 WARN 이 없다 (경계)")
        void noWarnAtThreshold(CapturedOutput output) {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 3);

            // When
            service.drawReadyDrops(CLOSE_AT.plus(WARN_THRESHOLD));

            // Then
            assertThat(warnLines(output, "dropId=" + DROP_ID)).isZero();
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.CLOSING);
        }

        @Test
        @DisplayName("[T5-30] 추첨 커밋 후 Gate 에 expireAfterDraw(dropId, 1d) 가 기록된다")
        void expiresKeysAfterDraw() {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 2);

            // When
            service.drawReadyDrops(NOW);

            // Then
            assertThat(gate.expireCalls()).containsExactly(new ExpireCall(DROP_ID, KEY_TTL));
        }

        @Test
        @DisplayName("[T5-30] expireAfterDraw 가 GateUnavailableException 이어도 예외 없이 추첨·발행 유지 (DRAWN, published=true)")
        void expireFailureKeepsDraw() {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            applicantCounter.set(DROP_ID, 2);
            gate.failExpireAfterDrawNext();

            // When
            assertThatCode(() -> service.drawReadyDrops(NOW)).doesNotThrowAnyException();

            // Then
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isTrue();
            assertThat(publisher.published()).hasSize(1);
            assertThat(gate.expireCalls()).hasSize(1);
        }
    }

    // ---------------------------------------------------------------- T5-28, T5-29 관리자 수동 추첨 (D5-1)

    @Nested
    @DisplayName("drawManually")
    class DrawManually {

        @Test
        @DisplayName("[T5-28] CLOSING, Redis 10 / DB 9, ADMIN → DB 9명 기준 추첨, 발행 1회, 결과 applicantCount=9, WARN(redis·db 수)")
        void manualDrawUsesDbEntries(CapturedOutput output) {
            // Given
            List<Long> userIds = givenClosingDrop(DROP_ID, 3, 9);
            applicantCounter.set(DROP_ID, 10);

            // When
            DrawSummaryResult summary = service.drawManually(manual("ADMIN", DROP_ID), NOW);

            // Then
            assertThat(summary).isEqualTo(new DrawSummaryResult(DROP_ID, 9, 3, SEED, NOW));
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.WON))
                    .containsExactlyInAnyOrderElementsOf(new Drawer().draw(userIds, 3, SEED));
            assertThat(usersWithStatus(DROP_ID, userIds, EntryStatus.LOST)).hasSize(6);
            assertThat(publisher.published()).containsExactly(new DrawCompletedPayload(DROP_ID, 9, 3, NOW));
            assertThat(results.findByDropId(DROP_ID).orElseThrow().isPublished()).isTrue();
            assertThat(gate.expireCalls()).containsExactly(new ExpireCall(DROP_ID, KEY_TTL));
            assertThat(warnLines(output, "dropId=" + DROP_ID, "redis=10", "db=9")).isEqualTo(1);
        }

        @Test
        @DisplayName("[T5-28] Redis 장애로 건수 조회가 실패해도 수동 추첨은 DB 기준으로 진행된다")
        void manualDrawIgnoresCounterFailure() {
            // Given
            givenClosingDrop(DROP_ID, 1, 4);
            applicantCounter.failAlways();

            // When
            DrawSummaryResult summary = service.drawManually(manual("ADMIN", DROP_ID), NOW);

            // Then
            assertThat(summary.applicantCount()).isEqualTo(4);
            assertThat(statusOf(DROP_ID)).isEqualTo(SnapshotStatus.DRAWN);
            assertThat(publisher.published()).hasSize(1);
        }

        @Test
        @DisplayName("[T5-28] 수동 추첨 발행 실패여도 요약을 돌려주고 published=false 로 남아 재발행 대상이 된다")
        void manualDrawPublishFailure() {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            publisher.failNext();

            // When
            DrawSummaryResult summary = service.drawManually(manual("ADMIN", DROP_ID), NOW);

            // Then
            assertThat(summary.dropId()).isEqualTo(DROP_ID);
            assertThat(results.findUnpublished()).extracting(DrawResult::getDropId).containsExactly(DROP_ID);
        }

        @Test
        @DisplayName("[T5-29] USER 역할 → 403 FORBIDDEN, 추첨·발행 없음")
        void userForbidden() {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("USER", DROP_ID), NOW))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
            assertNothingDrawn(SnapshotStatus.CLOSING);
        }

        @Test
        @DisplayName("[T5-29] 권한 검사가 먼저다: USER 가 없는 dropId 를 요청해도 403 (404 로 존재 여부를 흘리지 않음)")
        void forbiddenBeforeLookup() {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("USER", 999L), NOW))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
        }

        @Test
        @DisplayName("[T5-29] OPEN(마감 전) 드로우 → 409 ENTRY_DRAW_NOT_CLOSING, 추첨·발행 없음")
        void openDropNotClosing() {
            // Given
            givenOpenDrop(DROP_ID, CLOSE_AT);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("ADMIN", DROP_ID), NOW))
                    .satisfies(e -> assertErrorCode(e, DrawErrorCode.NOT_CLOSING));
            assertNothingDrawn(SnapshotStatus.OPEN);
        }

        @Test
        @DisplayName("[T5-29] DRAWN 드로우 → 409 ENTRY_DRAW_ALREADY_DRAWN, 추첨·발행 없음")
        void drawnDropAlreadyDrawn() {
            // Given
            givenClosingDrop(DROP_ID, 1, 2);
            snapshots.forceStatus(DROP_ID, SnapshotStatus.DRAWN);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("ADMIN", DROP_ID), NOW))
                    .satisfies(e -> assertErrorCode(e, DrawErrorCode.ALREADY_DRAWN));
            assertNothingDrawn(SnapshotStatus.DRAWN);
        }

        @Test
        @DisplayName("[T5-29] 없는 dropId → 404 ENTRY_DRAW_DROP_NOT_FOUND, 추첨·발행 없음")
        void missingDrop() {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("ADMIN", 999L), NOW))
                    .satisfies(e -> assertErrorCode(e, DrawErrorCode.DROP_NOT_FOUND));
            assertThat(results.count()).isZero();
            assertThat(publisher.attempted()).isEmpty();
            assertThat(seeds.calls()).isZero();
        }

        @Test
        @DisplayName("[T5-29] 상태 검사 직후 스케줄러가 먼저 추첨하면(DrawTransaction skipped) 409 ENTRY_DRAW_ALREADY_DRAWN, 수동 쪽 추첨·발행·키 만료 없음")
        void losesRaceToScheduler() {
            // Given: 검사 시점엔 CLOSING. 조건부 UPDATE 직전에 다른 인스턴스가 추첨을 끝낸다
            givenClosingDrop(DROP_ID, 1, 2);
            DrawResult schedulerResult = DrawResult.of(DROP_ID, 99L, 2, 1, NOW);
            snapshots.beforeNextMarkDrawn(() -> {
                snapshots.forceStatus(DROP_ID, SnapshotStatus.DRAWN);
                results.save(schedulerResult);
            });

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.drawManually(manual("ADMIN", DROP_ID), NOW))
                    .satisfies(e -> assertErrorCode(e, DrawErrorCode.ALREADY_DRAWN));
            assertThat(seeds.calls()).isZero();
            assertThat(results.count()).isEqualTo(1);
            assertThat(results.findByDropId(DROP_ID).orElseThrow().getSeed()).isEqualTo(99L);
            assertThat(publisher.attempted()).isEmpty();
            assertThat(gate.expireCalls()).isEmpty();
        }

        private void assertNothingDrawn(SnapshotStatus expectedStatus) {
            assertThat(statusOf(DROP_ID)).isEqualTo(expectedStatus);
            assertThat(results.count()).isZero();
            assertThat(publisher.attempted()).isEmpty();
            assertThat(seeds.calls()).isZero();
            assertThat(gate.expireCalls()).isEmpty();
        }
    }

    /** markDrawnIfClosing 직전에 끼어드는 동작(다른 인스턴스의 추첨)을 한 번 실행할 수 있는 저장소 */
    static class RacingSnapshotRepository extends InMemoryDropSnapshotRepository {

        private Runnable beforeNextMarkDrawn;

        void beforeNextMarkDrawn(Runnable action) {
            this.beforeNextMarkDrawn = action;
        }

        @Override
        public synchronized boolean markDrawnIfClosing(long dropId) {
            if (beforeNextMarkDrawn != null) {
                Runnable action = beforeNextMarkDrawn;
                beforeNextMarkDrawn = null;
                action.run();
            }
            return super.markDrawnIfClosing(dropId);
        }
    }
}
