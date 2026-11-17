package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.dto.DrawOutcome;
import com.queuedrop.entry.application.port.SeedGenerator;
import com.queuedrop.entry.application.service.fake.FixedSeedGenerator;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * DrawTransaction + 실제 PostgreSQL. 트랜잭션 경계(커밋·롤백)와 조건부 UPDATE 행 잠금을 보므로
 * 테스트 메서드 자체는 트랜잭션 밖에서 실행한다 (NOT_SUPPORTED). 매 테스트 전후로 테이블을 비운다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, DrawTransaction.class, DrawTransactionTestConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class DrawTransactionTest {

    static final long SEED = 42L;
    private static final long DROP_ID = 1L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-17T12:00:00Z");
    private static final Instant NOW = Instant.parse("2026-11-17T12:00:05Z");

    @Autowired DrawTransaction drawTransaction;
    @Autowired FailingEntryRepository failingEntries;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    @AfterEach
    void clean() {
        failingEntries.failMarkResults(false);
        jdbc.execute("TRUNCATE TABLE draw_results, entries, drop_snapshots");
    }

    private void givenDrop(long dropId, String status, int winnerCount, int applicantCount) {
        jdbc.update("""
                INSERT INTO drop_snapshots (drop_id, entry_capacity, winner_count, open_at, close_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """, dropId, Math.max(winnerCount, applicantCount), winnerCount,
                Timestamp.from(OPEN_AT), Timestamp.from(CLOSE_AT), status);
        List<Object[]> rows = new ArrayList<>();
        for (int seq = 1; seq <= applicantCount; seq++) {
            rows.add(new Object[] {dropId, dropId * 10_000 + seq, seq, Timestamp.from(OPEN_AT)});
        }
        jdbc.batchUpdate("""
                INSERT INTO entries (drop_id, user_id, entry_seq, status, applied_at)
                VALUES (?, ?, ?, 'APPLIED', ?)
                """, rows);
    }

    private String snapshotStatus(long dropId) {
        return jdbc.queryForObject("SELECT status FROM drop_snapshots WHERE drop_id = ?", String.class, dropId);
    }

    private int entriesWithStatus(long dropId, String status) {
        return jdbc.queryForObject("SELECT count(*) FROM entries WHERE drop_id = ? AND status = ?",
                Integer.class, dropId, status);
    }

    private int drawResultCount(long dropId) {
        return jdbc.queryForObject("SELECT count(*) FROM draw_results WHERE drop_id = ?", Integer.class, dropId);
    }

    // ---------------------------------------------------------------- T5-17

    @RepeatedTest(5)
    @DisplayName("[T5-17] CLOSING 드로우(응모 50, K=3)를 두 스레드가 동시에 execute → drawn=true 정확히 1개, 다른 쪽은 예외 없이 skipped, draw_results 1건")
    void concurrentExecuteDrawsOnce() throws Exception {
        // Given
        givenDrop(DROP_ID, "CLOSING", 3, 50);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<DrawOutcome>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    return drawTransaction.execute(DROP_ID, NOW);
                }));
            }
            ready.await(10, TimeUnit.SECONDS);

            // When
            start.countDown();
            List<DrawOutcome> outcomes = new ArrayList<>();
            for (Future<DrawOutcome> f : futures) {
                outcomes.add(f.get(30, TimeUnit.SECONDS));   // 어느 쪽이든 예외면 실패 (UK 위반으로 막히면 안 됨)
            }

            // Then
            assertThat(outcomes).filteredOn(DrawOutcome::drawn).singleElement()
                    .extracting(DrawOutcome::payload)
                    .isEqualTo(new DrawCompletedPayload(DROP_ID, 50, 3, NOW));
            assertThat(outcomes).filteredOn(o -> !o.drawn()).singleElement()
                    .extracting(DrawOutcome::payload).isNull();
            assertThat(drawResultCount(DROP_ID)).isEqualTo(1);
            assertThat(snapshotStatus(DROP_ID)).isEqualTo("DRAWN");
            assertThat(entriesWithStatus(DROP_ID, "WON")).isEqualTo(3);
            assertThat(entriesWithStatus(DROP_ID, "LOST")).isEqualTo(47);
            assertThat(entriesWithStatus(DROP_ID, "APPLIED")).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    // ---------------------------------------------------------------- T5-18

    @Test
    @DisplayName("[T5-18] markResults 가 벌크 UPDATE 를 실행한 뒤 예외 → 전체 롤백: 상태 CLOSING, entries 모두 APPLIED, draw_results 없음. 이후 재실행하면 추첨된다")
    void rollbackOnMarkResultsFailure() {
        // Given
        givenDrop(DROP_ID, "CLOSING", 2, 5);
        failingEntries.failMarkResults(true);

        // When / Then
        assertThatThrownBy(() -> drawTransaction.execute(DROP_ID, NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("injected");
        assertThat(snapshotStatus(DROP_ID)).isEqualTo("CLOSING");
        assertThat(entriesWithStatus(DROP_ID, "APPLIED")).isEqualTo(5);
        assertThat(drawResultCount(DROP_ID)).isZero();

        // When: 장애 해소 후 다음 주기 재시도
        failingEntries.failMarkResults(false);
        DrawOutcome retry = drawTransaction.execute(DROP_ID, NOW);

        // Then
        assertThat(retry.drawn()).isTrue();
        assertThat(snapshotStatus(DROP_ID)).isEqualTo("DRAWN");
        assertThat(entriesWithStatus(DROP_ID, "WON")).isEqualTo(2);
        assertThat(entriesWithStatus(DROP_ID, "LOST")).isEqualTo(3);
        assertThat(drawResultCount(DROP_ID)).isEqualTo(1);
    }

    @Test
    @DisplayName("[T5-17] OPEN·DRAWN 드로우는 skipped(drawn=false, payload=null) 이고 아무것도 바꾸지 않는다")
    void skipsWhenNotClosing() {
        // Given
        givenDrop(1L, "OPEN", 1, 2);
        givenDrop(2L, "DRAWN", 1, 2);

        // When
        DrawOutcome open = drawTransaction.execute(1L, NOW);
        DrawOutcome drawn = drawTransaction.execute(2L, NOW);

        // Then
        assertThat(open).isEqualTo(new DrawOutcome(false, null));
        assertThat(drawn).isEqualTo(new DrawOutcome(false, null));
        assertThat(snapshotStatus(1L)).isEqualTo("OPEN");
        assertThat(snapshotStatus(2L)).isEqualTo("DRAWN");
        assertThat(entriesWithStatus(1L, "APPLIED")).isEqualTo(2);
        assertThat(entriesWithStatus(2L, "APPLIED")).isEqualTo(2);
        assertThat(drawResultCount(1L) + drawResultCount(2L)).isZero();
    }
}

/** 실제 영속 어댑터 + 고정 시드 + markResults 실패 주입용 EntryRepository(@Primary) */
@TestConfiguration(proxyBeanMethods = false)
@ComponentScan(basePackages = "com.queuedrop.entry.infrastructure.persistence")
class DrawTransactionTestConfig {

    @Bean
    SeedGenerator seedGenerator() {
        return new FixedSeedGenerator(DrawTransactionTest.SEED);
    }

    @Bean
    @Primary
    FailingEntryRepository failingEntryRepository(@Qualifier("entryRepositoryImpl") EntryRepository delegate) {
        return new FailingEntryRepository(delegate);
    }
}

/** 실제 EntryRepository 에 위임하되, 실패 주입 시 markResults 의 벌크 UPDATE 를 실행한 뒤 예외를 던진다 (롤백 확인용) */
class FailingEntryRepository implements EntryRepository {

    private final EntryRepository delegate;
    private volatile boolean failMarkResults;

    FailingEntryRepository(EntryRepository delegate) {
        this.delegate = delegate;
    }

    void failMarkResults(boolean fail) {
        this.failMarkResults = fail;
    }

    @Override
    public boolean saveIfAbsent(Entry entry) {
        return delegate.saveIfAbsent(entry);
    }

    @Override
    public Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return delegate.findByDropIdAndUserId(dropId, userId);
    }

    @Override
    public List<Entry> findAllByUserId(long userId) {
        return delegate.findAllByUserId(userId);
    }

    @Override
    public long countByDropId(long dropId) {
        return delegate.countByDropId(dropId);
    }

    @Override
    public List<Long> findUserIdsOrderBySeq(long dropId) {
        return delegate.findUserIdsOrderBySeq(dropId);
    }

    @Override
    public void markResults(long dropId, Set<Long> winnerUserIds) {
        delegate.markResults(dropId, winnerUserIds);
        if (failMarkResults) {
            throw new IllegalStateException("injected markResults failure");
        }
    }
}
