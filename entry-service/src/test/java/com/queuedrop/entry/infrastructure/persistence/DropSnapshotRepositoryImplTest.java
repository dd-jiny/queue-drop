package com.queuedrop.entry.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, DropSnapshotRepositoryImpl.class})
class DropSnapshotRepositoryImplTest {

    /** PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘 시각. */
    private static final Instant OPEN_AT = Instant.parse("2026-11-17T10:00:00.000001Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-17T12:00:00.999999Z");

    @Autowired DropSnapshotRepositoryImpl dropSnapshotRepository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    // ---------------------------------------------------------------- T4-22

    @Test
    @DisplayName("[T4-22] 같은 dropId 를 두 번 저장하면 두 번째는 false 이고 첫 값이 유지된다")
    void secondSaveIgnored() {
        // Given
        boolean first = dropSnapshotRepository.saveIfAbsent(DropSnapshot.register(1L, 100, 10, OPEN_AT, CLOSE_AT));

        // When: 값이 다른 같은 dropId (drop.created 재수신 / 덮어쓰기 시도)
        boolean second = dropSnapshotRepository.saveIfAbsent(
                DropSnapshot.register(1L, 500, 50, OPEN_AT.plusSeconds(60), CLOSE_AT.plusSeconds(60)));
        entityManager.clear();

        // Then
        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(jdbcTemplate.queryForObject("select count(*) from drop_snapshots", Long.class)).isEqualTo(1L);
        DropSnapshot kept = dropSnapshotRepository.findById(1L).orElseThrow();
        assertThat(kept.getEntryCapacity()).isEqualTo(100);
        assertThat(kept.getWinnerCount()).isEqualTo(10);
        assertThat(kept.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(kept.getCloseAt()).isEqualTo(CLOSE_AT);
    }

    @Test
    @DisplayName("[T4-22] 저장 후 findById 로 읽으면 모든 필드(마이크로초 Instant, status=OPEN)가 일치한다")
    void saveAndFindRoundTrip() {
        // Given
        dropSnapshotRepository.saveIfAbsent(DropSnapshot.register(42L, 100, 7, OPEN_AT, CLOSE_AT));
        entityManager.clear();

        // When
        DropSnapshot found = dropSnapshotRepository.findById(42L).orElseThrow();

        // Then
        assertThat(found.getDropId()).isEqualTo(42L);
        assertThat(found.getEntryCapacity()).isEqualTo(100);
        assertThat(found.getWinnerCount()).isEqualTo(7);
        assertThat(found.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(found.getCloseAt()).isEqualTo(CLOSE_AT);
        assertThat(found.getStatus()).isEqualTo(SnapshotStatus.OPEN);
    }

    @Test
    @DisplayName("[T4-22] 없는 dropId 를 findById 하면 빈 값")
    void findMissing() {
        // When / Then
        assertThat(dropSnapshotRepository.findById(999_999L)).isEmpty();
    }

    // ---------------------------------------------------------------- 경계 보강 (장원영, 커밋 #4 검증)

    @Test
    @DisplayName("[T4-22] OPEN 이 아닌 상태(CLOSING)도 toEntity → INSERT → toDomain 왕복에서 유지된다")
    void nonOpenStatusRoundTrip() {
        // Given
        DropSnapshot closing = DropSnapshot.restore(7L, 100, 10, OPEN_AT, CLOSE_AT, SnapshotStatus.CLOSING);

        // When
        boolean saved = dropSnapshotRepository.saveIfAbsent(closing);
        entityManager.clear();

        // Then
        assertThat(saved).isTrue();
        assertThat(dropSnapshotRepository.findById(7L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.CLOSING);
    }

    @Test
    @DisplayName("[T4-22] 두 번째 저장이 상태까지 다른 값이어도 첫 상태가 유지된다 (상태 덮어쓰기 없음)")
    void secondSaveDoesNotOverwriteStatus() {
        // Given
        dropSnapshotRepository.saveIfAbsent(DropSnapshot.register(8L, 100, 10, OPEN_AT, CLOSE_AT));

        // When
        boolean second = dropSnapshotRepository.saveIfAbsent(
                DropSnapshot.restore(8L, 100, 10, OPEN_AT, CLOSE_AT, SnapshotStatus.DRAWN));
        entityManager.clear();

        // Then
        assertThat(second).isFalse();
        assertThat(dropSnapshotRepository.findById(8L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.OPEN);
    }

    // ================================================================ 5단계 — 조건부 상태 전이

    private static final Instant AFTER_CLOSE = CLOSE_AT.plusSeconds(1);

    private void insert(long dropId, SnapshotStatus status, Instant closeAt) {
        dropSnapshotRepository.saveIfAbsent(DropSnapshot.restore(dropId, 100, 10, OPEN_AT, closeAt, status));
    }

    private String statusOf(long dropId) {
        return jdbcTemplate.queryForObject("select status from drop_snapshots where drop_id = ?", String.class, dropId);
    }

    // ---------------------------------------------------------------- T5-14

    /**
     * 두 스레드가 각자 트랜잭션을 커밋해야 하므로 테스트 트랜잭션을 끈다.
     * 커밋된 행은 롤백되지 않으니 finally 에서 직접 지운다.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @DisplayName("[T5-14] OPEN 드로우에 스레드 2개가 동시에 markClosingIfOpen 하면 true 는 정확히 1개, 상태는 CLOSING")
    void concurrentMarkClosingOnlyOneWins() throws Exception {
        long dropId = 51_401L;
        try {
            // Given: 커밋된 OPEN 드로우 (마감 시각 지남)
            insert(dropId, SnapshotStatus.OPEN, CLOSE_AT);
            int threads = 2;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();

            // When
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                Callable<Boolean> task = () -> {
                    ready.countDown();
                    start.await();
                    return dropSnapshotRepository.markClosingIfOpen(dropId, AFTER_CLOSE);
                };
                for (int i = 0; i < threads; i++) {
                    results.add(pool.submit(task));
                }
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
            }

            // Then
            List<Boolean> outcomes = new ArrayList<>();
            for (Future<Boolean> f : results) {
                outcomes.add(f.get(10, TimeUnit.SECONDS));
            }
            assertThat(outcomes).containsExactlyInAnyOrder(true, false);
            assertThat(statusOf(dropId)).isEqualTo("CLOSING");
        } finally {
            jdbcTemplate.update("delete from drop_snapshots where drop_id = ?", dropId);
        }
    }

    @Test
    @DisplayName("[T5-14] markClosingIfOpen — closeAt == now 이면 전이한다 (마감 경계 포함)")
    void markClosingAtExactCloseAt() {
        // Given
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);

        // When
        boolean changed = dropSnapshotRepository.markClosingIfOpen(1L, CLOSE_AT);

        // Then
        assertThat(changed).isTrue();
        assertThat(statusOf(1L)).isEqualTo("CLOSING");
    }

    @Test
    @DisplayName("[T5-14] markClosingIfOpen — 마감 전(now < closeAt)이면 false 이고 OPEN 유지")
    void markClosingBeforeCloseAtIgnored() {
        // Given
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);

        // When
        boolean changed = dropSnapshotRepository.markClosingIfOpen(1L, CLOSE_AT.minusNanos(1_000));

        // Then
        assertThat(changed).isFalse();
        assertThat(statusOf(1L)).isEqualTo("OPEN");
    }

    @Test
    @DisplayName("[T5-14] markClosingIfOpen — 이미 CLOSING / DRAWN 이거나 없는 dropId 면 false, 상태 그대로")
    void markClosingNonOpenIgnored() {
        // Given
        insert(1L, SnapshotStatus.CLOSING, CLOSE_AT);
        insert(2L, SnapshotStatus.DRAWN, CLOSE_AT);

        // When / Then
        assertThat(dropSnapshotRepository.markClosingIfOpen(1L, AFTER_CLOSE)).isFalse();
        assertThat(dropSnapshotRepository.markClosingIfOpen(2L, AFTER_CLOSE)).isFalse();
        assertThat(dropSnapshotRepository.markClosingIfOpen(999_999L, AFTER_CLOSE)).isFalse();
        assertThat(statusOf(1L)).isEqualTo("CLOSING");
        assertThat(statusOf(2L)).isEqualTo("DRAWN");
    }

    @Test
    @DisplayName("[T5-14] findDueOpenIds — OPEN 이고 closeAt <= now 인 id 만 (경계 포함, 마감 전·CLOSING·DRAWN 제외)")
    void findDueOpenIds() {
        // Given
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);                       // 경계: closeAt == now
        insert(2L, SnapshotStatus.OPEN, CLOSE_AT.minusSeconds(3600));    // 마감 지남
        insert(3L, SnapshotStatus.OPEN, CLOSE_AT.plusSeconds(1));        // 마감 전
        insert(4L, SnapshotStatus.CLOSING, CLOSE_AT.minusSeconds(60));
        insert(5L, SnapshotStatus.DRAWN, CLOSE_AT.minusSeconds(60));

        // When
        List<Long> due = dropSnapshotRepository.findDueOpenIds(CLOSE_AT);

        // Then
        assertThat(due).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    @DisplayName("[T5-14] findClosing — CLOSING 스냅샷만 모든 필드가 복원되어 돌아온다")
    void findClosing() {
        // Given
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);
        dropSnapshotRepository.saveIfAbsent(DropSnapshot.restore(2L, 300, 30, OPEN_AT, CLOSE_AT, SnapshotStatus.CLOSING));
        insert(3L, SnapshotStatus.DRAWN, CLOSE_AT);
        entityManager.clear();

        // When
        List<DropSnapshot> closing = dropSnapshotRepository.findClosing();

        // Then
        assertThat(closing).hasSize(1);
        DropSnapshot found = closing.getFirst();
        assertThat(found.getDropId()).isEqualTo(2L);
        assertThat(found.getEntryCapacity()).isEqualTo(300);
        assertThat(found.getWinnerCount()).isEqualTo(30);
        assertThat(found.getCloseAt()).isEqualTo(CLOSE_AT);
        assertThat(found.getStatus()).isEqualTo(SnapshotStatus.CLOSING);
    }

    @Test
    @DisplayName("[T5-14] markDrawnIfClosing — CLOSING 이면 true·DRAWN, 두 번째 호출은 false")
    void markDrawnIfClosing() {
        // Given
        insert(1L, SnapshotStatus.CLOSING, CLOSE_AT);

        // When
        boolean first = dropSnapshotRepository.markDrawnIfClosing(1L);
        boolean second = dropSnapshotRepository.markDrawnIfClosing(1L);

        // Then
        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(statusOf(1L)).isEqualTo("DRAWN");
        entityManager.clear();
        assertThat(dropSnapshotRepository.findById(1L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.DRAWN);
    }

    @Test
    @DisplayName("[T5-14] markDrawnIfClosing — OPEN 이거나 없는 dropId 면 false (OPEN 을 건너뛰어 DRAWN 으로 가지 않음)")
    void markDrawnFromOpenIgnored() {
        // Given
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);

        // When / Then
        assertThat(dropSnapshotRepository.markDrawnIfClosing(1L)).isFalse();
        assertThat(dropSnapshotRepository.markDrawnIfClosing(999_999L)).isFalse();
        assertThat(statusOf(1L)).isEqualTo("OPEN");
    }

    @Test
    @DisplayName("[T5-14] 같은 트랜잭션에서 먼저 읽은 뒤 markClosingIfOpen·markDrawnIfClosing 해도 findById 는 새 상태 (clearAutomatically, 수동 clear 없음)")
    void conditionalUpdatesClearPersistenceContext() {
        // Given: 먼저 읽어 영속성 컨텍스트에 OPEN 엔티티를 올려 둔다
        insert(1L, SnapshotStatus.OPEN, CLOSE_AT);
        assertThat(dropSnapshotRepository.findById(1L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.OPEN);

        // When / Then: entityManager.clear() 없이 다시 읽는다
        assertThat(dropSnapshotRepository.markClosingIfOpen(1L, AFTER_CLOSE)).isTrue();
        assertThat(dropSnapshotRepository.findById(1L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.CLOSING);
        assertThat(dropSnapshotRepository.findClosing()).extracting(DropSnapshot::getDropId).containsExactly(1L);

        assertThat(dropSnapshotRepository.markDrawnIfClosing(1L)).isTrue();
        assertThat(dropSnapshotRepository.findById(1L).orElseThrow().getStatus()).isEqualTo(SnapshotStatus.DRAWN);
        assertThat(dropSnapshotRepository.findClosing()).isEmpty();
    }
}
