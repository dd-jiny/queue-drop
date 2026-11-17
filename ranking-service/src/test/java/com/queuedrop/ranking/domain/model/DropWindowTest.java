package com.queuedrop.ranking.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DropWindowTest {

    private static final long DROP_ID = 1L;
    private static final Instant CLOSE_AT = Instant.parse("2026-11-17T12:00:00Z");

    @Test
    @DisplayName("[T7-07] closeAt 1ms 전이면 진행 중이고 종료가 아니다")
    void justBeforeCloseAtIsInProgress() {
        // Given
        DropWindow window = new DropWindow(DROP_ID, CLOSE_AT);
        Instant now = CLOSE_AT.minusMillis(1);

        // When / Then
        assertThat(window.isInProgress(now)).isTrue();
        assertThat(window.isClosed(now)).isFalse();
    }

    @Test
    @DisplayName("[T7-07] closeAt 정각이면 종료다")
    void atCloseAtIsClosed() {
        // Given
        DropWindow window = new DropWindow(DROP_ID, CLOSE_AT);

        // When / Then
        assertThat(window.isInProgress(CLOSE_AT)).isFalse();
        assertThat(window.isClosed(CLOSE_AT)).isTrue();
    }

    @Test
    @DisplayName("[T7-07] closeAt 이후면 종료다")
    void afterCloseAtIsClosed() {
        // Given
        DropWindow window = new DropWindow(DROP_ID, CLOSE_AT);
        Instant now = CLOSE_AT.plusSeconds(1);

        // When / Then
        assertThat(window.isInProgress(now)).isFalse();
        assertThat(window.isClosed(now)).isTrue();
    }

    @Test
    @DisplayName("[T7-07] closeAt 을 모르면(null) 진행 중으로 간주하고 종료가 아니다")
    void unknownCloseAtIsInProgress() {
        // Given
        DropWindow window = new DropWindow(DROP_ID, null);
        Instant now = CLOSE_AT.plusSeconds(86_400);

        // When / Then
        assertThat(window.isInProgress(now)).isTrue();
        assertThat(window.isClosed(now)).isFalse();
    }
}
