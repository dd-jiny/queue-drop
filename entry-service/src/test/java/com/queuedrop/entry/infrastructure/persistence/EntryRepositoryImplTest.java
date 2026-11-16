package com.queuedrop.entry.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, EntryRepositoryImpl.class})
class EntryRepositoryImplTest {

    /** PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘 시각. */
    private static final Instant APPLIED_AT = Instant.parse("2026-11-17T10:00:00.123456Z");

    @Autowired EntryRepositoryImpl entryRepository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    private long countEntries(long dropId) {
        return jdbcTemplate.queryForObject("select count(*) from entries where drop_id = ?", Long.class, dropId);
    }

    // ---------------------------------------------------------------- T4-19

    @Test
    @DisplayName("[T4-19] 같은 (drop, user) 로 다른 seq 를 다시 저장하면 예외 없이 false 이고 1건만 남는다")
    void duplicateUserIgnored() {
        // Given
        assertThat(entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT))).isTrue();

        // When
        boolean[] second = new boolean[1];
        assertThatCode(() -> second[0] = entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 2, APPLIED_AT.plusSeconds(1))))
                .doesNotThrowAnyException();

        // Then: 첫 값이 유지된다
        assertThat(second[0]).isFalse();
        assertThat(countEntries(1L)).isEqualTo(1L);
        entityManager.clear();
        Entry kept = entryRepository.findByDropIdAndUserId(1L, 1L).orElseThrow();
        assertThat(kept.getEntrySeq()).isEqualTo(1);
        assertThat(kept.getAppliedAt()).isEqualTo(APPLIED_AT);
    }

    @Test
    @DisplayName("[T4-19] 같은 사용자라도 드로우가 다르면 각각 저장된다 (충돌 대상은 (drop, user))")
    void sameUserOtherDropSaved() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT));

        // When
        boolean saved = entryRepository.saveIfAbsent(Entry.apply(2L, 1L, 1, APPLIED_AT));

        // Then
        assertThat(saved).isTrue();
        assertThat(countEntries(1L)).isEqualTo(1L);
        assertThat(countEntries(2L)).isEqualTo(1L);
    }

    // ---------------------------------------------------------------- T4-20

    /** PostgreSQL 은 실패한 문장 이후 트랜잭션이 중단되므로 위반 INSERT 뒤에는 쿼리하지 않는다. */
    @Test
    @DisplayName("[T4-20] 같은 드로우에서 다른 사용자가 같은 seq 로 저장하면 DataIntegrityViolationException (seq 충돌은 숨기지 않음)")
    void seqConflictThrows() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT));

        // When / Then
        assertThatThrownBy(() -> entryRepository.saveIfAbsent(Entry.apply(1L, 2L, 1, APPLIED_AT)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    // ---------------------------------------------------------------- T4-21

    @Test
    @DisplayName("[T4-21] 저장 후 findByDropIdAndUserId 로 읽으면 id 가 부여되고 모든 필드(마이크로초 Instant 포함)가 일치한다")
    void findByDropIdAndUserIdRoundTrip() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(10L, 20L, 7, APPLIED_AT));
        entityManager.clear();

        // When
        Entry found = entryRepository.findByDropIdAndUserId(10L, 20L).orElseThrow();

        // Then
        assertThat(found.getId()).isNotNull();
        assertThat(found.getDropId()).isEqualTo(10L);
        assertThat(found.getUserId()).isEqualTo(20L);
        assertThat(found.getEntrySeq()).isEqualTo(7);
        assertThat(found.getStatus()).isEqualTo(EntryStatus.APPLIED);
        assertThat(found.getAppliedAt()).isEqualTo(APPLIED_AT);
    }

    @Test
    @DisplayName("[T4-21] 없는 (drop, user) 는 빈 값")
    void findMissing() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(10L, 20L, 1, APPLIED_AT));

        // When / Then
        assertThat(entryRepository.findByDropIdAndUserId(10L, 21L)).isEmpty();
        assertThat(entryRepository.findByDropIdAndUserId(11L, 20L)).isEmpty();
    }

    @Test
    @DisplayName("[T4-21] findAllByUserId 는 해당 사용자 응모만 appliedAt 최신순으로 돌려준다 (저장 순서와 무관)")
    void findAllByUserIdLatestFirst() {
        // Given: 저장 순서를 appliedAt 순서와 다르게 둔다. 다른 사용자 응모도 섞는다.
        entryRepository.saveIfAbsent(Entry.apply(1L, 5L, 1, APPLIED_AT.plusSeconds(60)));
        entryRepository.saveIfAbsent(Entry.apply(2L, 5L, 1, APPLIED_AT.plusSeconds(180)));
        entryRepository.saveIfAbsent(Entry.apply(3L, 5L, 1, APPLIED_AT));
        entryRepository.saveIfAbsent(Entry.apply(1L, 6L, 2, APPLIED_AT.plusSeconds(300)));
        entityManager.clear();

        // When
        List<Entry> mine = entryRepository.findAllByUserId(5L);

        // Then
        assertThat(mine).extracting(Entry::getDropId).containsExactly(2L, 1L, 3L);
        assertThat(mine).allSatisfy(e -> assertThat(e.getUserId()).isEqualTo(5L));
        assertThat(mine.getFirst().getAppliedAt()).isEqualTo(APPLIED_AT.plusSeconds(180));
        assertThat(entryRepository.findAllByUserId(999L)).isEmpty();
    }

    // ---------------------------------------------------------------- 경계 보강 (장원영, 커밋 #4 검증)

    @Test
    @DisplayName("[T4-19] 같은 이벤트 재전달 — (drop, user, seq) 가 모두 같아도 seq 제약 예외 없이 false, 1건 유지")
    void exactRedeliveryIgnored() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT));

        // When: 두 유니크 제약에 동시에 걸리는 행. 중재 인덱스 (drop_id, user_id) 가 먼저 판정되어야 한다
        boolean[] second = new boolean[1];
        assertThatCode(() -> second[0] = entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT)))
                .doesNotThrowAnyException();

        // Then
        assertThat(second[0]).isFalse();
        assertThat(countEntries(1L)).isEqualTo(1L);
    }

    @Test
    @DisplayName("[T4-20] 다른 드로우라면 같은 seq 도 충돌하지 않는다 (seq 유니크 범위는 드로우 단위)")
    void sameSeqOtherDropSaved() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT));

        // When
        boolean saved = entryRepository.saveIfAbsent(Entry.apply(2L, 2L, 1, APPLIED_AT));

        // Then
        assertThat(saved).isTrue();
        assertThat(countEntries(2L)).isEqualTo(1L);
    }

    @Test
    @DisplayName("[T4-21] DB 의 WON/LOST 상태도 Mapper 를 거쳐 도메인 상태로 그대로 복원된다")
    void nonAppliedStatusMapped() {
        // Given: 5단계 추첨 결과를 흉내 내 상태만 직접 바꾼다
        entryRepository.saveIfAbsent(Entry.apply(1L, 1L, 1, APPLIED_AT));
        entryRepository.saveIfAbsent(Entry.apply(1L, 2L, 2, APPLIED_AT));
        jdbcTemplate.update("update entries set status = 'WON' where user_id = 1");
        jdbcTemplate.update("update entries set status = 'LOST' where user_id = 2");
        entityManager.clear();

        // When / Then
        assertThat(entryRepository.findByDropIdAndUserId(1L, 1L).orElseThrow().getStatus()).isEqualTo(EntryStatus.WON);
        assertThat(entryRepository.findByDropIdAndUserId(1L, 2L).orElseThrow().getStatus()).isEqualTo(EntryStatus.LOST);
    }

    @Test
    @DisplayName("[T4-21] findAllByUserId — appliedAt 동률이어도 누락·중복 없이 모두 돌려준다 (동률 순서는 미정의)")
    void findAllByUserIdTie() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, 5L, 1, APPLIED_AT));
        entryRepository.saveIfAbsent(Entry.apply(2L, 5L, 1, APPLIED_AT));
        entryRepository.saveIfAbsent(Entry.apply(3L, 5L, 1, APPLIED_AT.plusSeconds(1)));
        entityManager.clear();

        // When
        List<Entry> mine = entryRepository.findAllByUserId(5L);

        // Then
        assertThat(mine).extracting(Entry::getDropId).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(mine.getFirst().getDropId()).isEqualTo(3L);
    }
}
