package com.queuedrop.entry.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

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
}
