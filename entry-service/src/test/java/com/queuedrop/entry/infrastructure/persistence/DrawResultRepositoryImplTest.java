package com.queuedrop.entry.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.entry.domain.model.DrawResult;
import java.time.Instant;
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
@Import({PostgresContainerConfig.class, DrawResultRepositoryImpl.class})
class DrawResultRepositoryImplTest {

    /** PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘 시각. */
    private static final Instant DRAWN_AT = Instant.parse("2026-11-17T12:00:01.654321Z");

    @Autowired DrawResultRepositoryImpl drawResultRepository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    // ---------------------------------------------------------------- T5-34

    @Test
    @DisplayName("[T5-34] save 하면 id 가 부여되고 findByDropId 로 읽은 값이 모든 필드(음수 seed, 마이크로초 Instant, published=false)와 일치한다")
    void saveAndFindRoundTrip() {
        // Given: seed 는 long 전 범위를 쓰므로 음수·큰 값도 손실 없이 저장돼야 한다
        DrawResult origin = DrawResult.of(5L, Long.MIN_VALUE + 7, 987, 10, DRAWN_AT);

        // When
        DrawResult saved = drawResultRepository.save(origin);
        entityManager.flush();
        entityManager.clear();
        DrawResult found = drawResultRepository.findByDropId(5L).orElseThrow();

        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(found.getId()).isEqualTo(saved.getId());
        assertThat(found.getDropId()).isEqualTo(5L);
        assertThat(found.getSeed()).isEqualTo(Long.MIN_VALUE + 7);
        assertThat(found.getApplicantCount()).isEqualTo(987);
        assertThat(found.getWinnerCount()).isEqualTo(10);
        assertThat(found.getDrawnAt()).isEqualTo(DRAWN_AT);
        assertThat(found.isPublished()).isFalse();
    }

    @Test
    @DisplayName("[T5-34] published=true 로 저장한 기록은 published=true 로 복원된다 (Mapper 가 published 를 버리지 않음)")
    void publishedFlagRoundTrip() {
        // Given
        drawResultRepository.save(DrawResult.of(6L, 1L, 3, 3, DRAWN_AT).markPublished());
        entityManager.flush();
        entityManager.clear();

        // When
        DrawResult found = drawResultRepository.findByDropId(6L).orElseThrow();

        // Then
        assertThat(found.isPublished()).isTrue();
    }

    @Test
    @DisplayName("[T5-34] 응모자 0명 기록(applicantCount=0, winnerCount=0)도 저장된다")
    void zeroApplicantsSaved() {
        // When
        drawResultRepository.save(DrawResult.of(7L, 0L, 0, 0, DRAWN_AT));
        entityManager.flush();
        entityManager.clear();

        // Then
        DrawResult found = drawResultRepository.findByDropId(7L).orElseThrow();
        assertThat(found.getApplicantCount()).isZero();
        assertThat(found.getWinnerCount()).isZero();
    }

    @Test
    @DisplayName("[T5-34] 없는 dropId 를 findByDropId 하면 빈 값")
    void findMissing() {
        // When / Then
        assertThat(drawResultRepository.findByDropId(999_999L)).isEmpty();
    }

    @Test
    @DisplayName("[T5-34] findUnpublished 는 published=false 기록만 돌려준다")
    void findUnpublished() {
        // Given
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));
        drawResultRepository.save(DrawResult.of(2L, 22L, 10, 3, DRAWN_AT).markPublished());
        drawResultRepository.save(DrawResult.of(3L, 33L, 10, 3, DRAWN_AT));
        entityManager.flush();
        entityManager.clear();

        // When / Then
        assertThat(drawResultRepository.findUnpublished())
                .extracting(DrawResult::getDropId)
                .containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    @DisplayName("[T5-34] markPublished 후 published=true 로 읽히고 findUnpublished 에서 빠진다. 다른 드로우는 그대로")
    void markPublished() {
        // Given
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));
        drawResultRepository.save(DrawResult.of(2L, 22L, 10, 3, DRAWN_AT));
        entityManager.flush();
        entityManager.clear();

        // When
        drawResultRepository.markPublished(1L);
        entityManager.flush();
        entityManager.clear();

        // Then
        assertThat(jdbcTemplate.queryForObject(
                "select published from draw_results where drop_id = 1", Boolean.class)).isTrue();
        assertThat(drawResultRepository.findByDropId(1L).orElseThrow().isPublished()).isTrue();
        assertThat(drawResultRepository.findByDropId(2L).orElseThrow().isPublished()).isFalse();
        assertThat(drawResultRepository.findUnpublished())
                .extracting(DrawResult::getDropId)
                .containsExactly(2L);
    }

    @Test
    @DisplayName("[T5-34] markPublished 를 두 번 호출해도 예외 없이 published=true 유지 (재발행 멱등)")
    void markPublishedTwice() {
        // Given
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));
        entityManager.flush();
        entityManager.clear();

        // When
        drawResultRepository.markPublished(1L);
        drawResultRepository.markPublished(1L);
        entityManager.flush();
        entityManager.clear();

        // Then
        assertThat(drawResultRepository.findByDropId(1L).orElseThrow().isPublished()).isTrue();
    }

    /** PostgreSQL 은 실패한 문장 이후 트랜잭션이 중단되므로 위반 INSERT 뒤에는 쿼리하지 않는다. */
    @Test
    @DisplayName("[T5-34] 같은 dropId 로 두 번 save 하면 DataIntegrityViolationException (uk_draw_results_drop_id)")
    void duplicateDropIdThrows() {
        // Given
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));
        entityManager.flush();

        // When / Then: 예외가 flush 시점까지 미뤄지지 않고 save 호출에서 (번역된 예외로) 드러나야 한다
        assertThatThrownBy(() -> drawResultRepository.save(DrawResult.of(1L, 22L, 10, 3, DRAWN_AT)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("[T5-34] save 직후 같은 트랜잭션에서 markPublished 해도 findByDropId 는 published=true (clearAutomatically, 수동 clear 없음)")
    void markPublishedClearsPersistenceContext() {
        // Given: save 로 published=false 엔티티가 영속성 컨텍스트에 남아 있다
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));
        assertThat(drawResultRepository.findByDropId(1L).orElseThrow().isPublished()).isFalse();

        // When
        drawResultRepository.markPublished(1L);

        // Then: entityManager.clear() 없이 다시 읽는다
        assertThat(drawResultRepository.findByDropId(1L).orElseThrow().isPublished()).isTrue();
        assertThat(drawResultRepository.findUnpublished()).isEmpty();
    }

    @Test
    @DisplayName("[T5-34] 없는 dropId 를 markPublished 해도 예외 없고 다른 기록은 그대로")
    void markPublishedUnknownDropIdIsNoop() {
        // Given
        drawResultRepository.save(DrawResult.of(1L, 11L, 10, 3, DRAWN_AT));

        // When / Then
        assertThatCode(() -> drawResultRepository.markPublished(999_999L)).doesNotThrowAnyException();
        assertThat(drawResultRepository.findByDropId(1L).orElseThrow().isPublished()).isFalse();
    }
}
