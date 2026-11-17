package com.queuedrop.entry.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DrawResultTest {

    private static final Long DROP_ID = 10L;
    private static final long SEED = 42L;
    private static final Instant DRAWN_AT = Instant.parse("2026-11-16T12:00:30Z");

    @Test
    @DisplayName("[T5-33] applicantCount=-1 이면 DomainException")
    void negativeApplicantCountThrows() {
        // When / Then
        assertThatThrownBy(() -> DrawResult.of(DROP_ID, SEED, -1, 0, DRAWN_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T5-33] winnerCount=-1 이면 DomainException")
    void negativeWinnerCountThrows() {
        // When / Then
        assertThatThrownBy(() -> DrawResult.of(DROP_ID, SEED, 10, -1, DRAWN_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T5-33] winnerCount > applicantCount 이면 DomainException")
    void winnerCountGreaterThanApplicantCountThrows() {
        // When / Then
        assertThatThrownBy(() -> DrawResult.of(DROP_ID, SEED, 3, 4, DRAWN_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T5-33] 정상 값으로 생성하면 published=false 이고 입력 값을 그대로 갖는다")
    void ofWithValidValues() {
        // Given / When
        DrawResult result = DrawResult.of(DROP_ID, SEED, 10, 3, DRAWN_AT);

        // Then
        assertThat(result.isPublished()).isFalse();
        assertThat(result.getDropId()).isEqualTo(DROP_ID);
        assertThat(result.getSeed()).isEqualTo(SEED);
        assertThat(result.getApplicantCount()).isEqualTo(10);
        assertThat(result.getWinnerCount()).isEqualTo(3);
        assertThat(result.getDrawnAt()).isEqualTo(DRAWN_AT);
    }

    @Test
    @DisplayName("[T5-33] 경계값 applicantCount=0, winnerCount=0(응모자 0명 추첨) 이면 생성된다")
    void ofWithZeroApplicants() {
        // Given / When
        DrawResult result = DrawResult.of(DROP_ID, SEED, 0, 0, DRAWN_AT);

        // Then
        assertThat(result.getApplicantCount()).isZero();
        assertThat(result.getWinnerCount()).isZero();
        assertThat(result.isPublished()).isFalse();
    }

    @Test
    @DisplayName("[T5-33] 경계값 winnerCount == applicantCount 이면 생성된다")
    void ofWithWinnerEqualToApplicants() {
        // Given / When
        DrawResult result = DrawResult.of(DROP_ID, SEED, 3, 3, DRAWN_AT);

        // Then
        assertThat(result.getWinnerCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("[T5-33] markPublished 는 published=true 인 새 인스턴스를 돌려주고 원본은 바뀌지 않는다")
    void markPublishedReturnsNewInstance() {
        // Given
        DrawResult original = DrawResult.of(DROP_ID, SEED, 10, 3, DRAWN_AT);

        // When
        DrawResult published = original.markPublished();

        // Then
        assertThat(published).isNotSameAs(original);
        assertThat(published.isPublished()).isTrue();
        assertThat(original.isPublished()).isFalse();
        assertThat(published.getId()).isEqualTo(original.getId());
        assertThat(published.getDropId()).isEqualTo(DROP_ID);
        assertThat(published.getSeed()).isEqualTo(SEED);
        assertThat(published.getApplicantCount()).isEqualTo(10);
        assertThat(published.getWinnerCount()).isEqualTo(3);
        assertThat(published.getDrawnAt()).isEqualTo(DRAWN_AT);
    }
}
