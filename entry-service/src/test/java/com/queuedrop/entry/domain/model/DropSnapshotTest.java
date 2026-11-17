package com.queuedrop.entry.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DropSnapshotTest {

    private static final Long DROP_ID = 10L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");

    @Test
    @DisplayName("[T4-02] 정상 값으로 register 하면 status=OPEN 이고 입력 값을 그대로 갖는다")
    void registerWithValidValues() {
        // Given / When
        DropSnapshot snapshot = DropSnapshot.register(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT);

        // Then
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.OPEN);
        assertThat(snapshot.getDropId()).isEqualTo(DROP_ID);
        assertThat(snapshot.getEntryCapacity()).isEqualTo(100);
        assertThat(snapshot.getWinnerCount()).isEqualTo(10);
        assertThat(snapshot.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(snapshot.getCloseAt()).isEqualTo(CLOSE_AT);
    }

    @Test
    @DisplayName("[T4-02] entryCapacity == winnerCount == 1(경계)이면 생성된다")
    void registerWithCapacityEqualToWinner() {
        // Given / When
        DropSnapshot snapshot = DropSnapshot.register(DROP_ID, 1, 1, OPEN_AT, CLOSE_AT);

        // Then
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.OPEN);
    }

    @Test
    @DisplayName("[T4-02] entryCapacity < winnerCount 이면 DomainException")
    void capacityLessThanWinnerThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 9, 10, OPEN_AT, CLOSE_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] winnerCount=0 이면 DomainException")
    void zeroWinnerThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, 0, OPEN_AT, CLOSE_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] openAt == closeAt 이면 DomainException")
    void openAtEqualsCloseAtThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, 10, OPEN_AT, OPEN_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] openAt > closeAt 이면 DomainException")
    void openAtAfterCloseAtThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, 10, CLOSE_AT, OPEN_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] dropId 가 null 이면 DomainException")
    void nullDropIdThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(null, 100, 10, OPEN_AT, CLOSE_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] openAt 이 null 이면 DomainException")
    void nullOpenAtThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, 10, null, CLOSE_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] closeAt 이 null 이면 DomainException")
    void nullCloseAtThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, 10, OPEN_AT, null))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] winnerCount 가 음수이면 DomainException")
    void negativeWinnerThrows() {
        // When / Then
        assertThatThrownBy(() -> DropSnapshot.register(DROP_ID, 100, -1, OPEN_AT, CLOSE_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-02] restore 는 검증 없이 저장된 상태를 그대로 복원한다")
    void restorePreservesValues() {
        // Given
        SnapshotStatus anyStatus = SnapshotStatus.values()[SnapshotStatus.values().length - 1];

        // When
        DropSnapshot snapshot = DropSnapshot.restore(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT, anyStatus);

        // Then
        assertThat(snapshot.getDropId()).isEqualTo(DROP_ID);
        assertThat(snapshot.getEntryCapacity()).isEqualTo(100);
        assertThat(snapshot.getWinnerCount()).isEqualTo(10);
        assertThat(snapshot.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(snapshot.getCloseAt()).isEqualTo(CLOSE_AT);
        assertThat(snapshot.getStatus()).isEqualTo(anyStatus);
    }

    private static DropSnapshot withStatus(SnapshotStatus status) {
        return DropSnapshot.restore(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT, status);
    }

    @Test
    @DisplayName("[T5-07] OPEN, closeAt=12:00 이면 isDue(11:59:59)=false, isDue(12:00)=true")
    void isDueAtCloseAtBoundary() {
        // Given
        DropSnapshot snapshot = withStatus(SnapshotStatus.OPEN);

        // When
        boolean beforeClose = snapshot.isDue(CLOSE_AT.minusSeconds(1));
        boolean atClose = snapshot.isDue(CLOSE_AT);

        // Then
        assertThat(beforeClose).isFalse();
        assertThat(atClose).isTrue();
    }

    @Test
    @DisplayName("[T5-07] OPEN 이 아니면(CLOSING·DRAWN) 마감 시각이 지나도 isDue=false")
    void isDueFalseWhenNotOpen() {
        // Given
        Instant afterClose = CLOSE_AT.plusSeconds(60);

        // When / Then
        assertThat(withStatus(SnapshotStatus.CLOSING).isDue(afterClose)).isFalse();
        assertThat(withStatus(SnapshotStatus.DRAWN).isDue(afterClose)).isFalse();
    }

    @Test
    @DisplayName("[T5-08] CLOSING, closeAt=12:00, 기준 5분이면 isClosingTooLong(12:05)=false, (12:05:01)=true")
    void isClosingTooLongBoundary() {
        // Given
        DropSnapshot snapshot = withStatus(SnapshotStatus.CLOSING);
        Duration threshold = Duration.ofMinutes(5);

        // When
        boolean atThreshold = snapshot.isClosingTooLong(CLOSE_AT.plus(threshold), threshold);
        boolean overThreshold = snapshot.isClosingTooLong(CLOSE_AT.plus(threshold).plusSeconds(1), threshold);

        // Then
        assertThat(atThreshold).isFalse();
        assertThat(overThreshold).isTrue();
    }

    @Test
    @DisplayName("[T5-08] CLOSING 이 아니면(OPEN·DRAWN) 기준을 넘겨도 isClosingTooLong=false")
    void isClosingTooLongFalseWhenNotClosing() {
        // Given
        Duration threshold = Duration.ofMinutes(5);
        Instant longAfter = CLOSE_AT.plus(Duration.ofHours(1));

        // When / Then
        assertThat(withStatus(SnapshotStatus.OPEN).isClosingTooLong(longAfter, threshold)).isFalse();
        assertThat(withStatus(SnapshotStatus.DRAWN).isClosingTooLong(longAfter, threshold)).isFalse();
    }

    @Test
    @DisplayName("[T5-08] isClosing / isDrawn 은 상태별로 OPEN(false,false), CLOSING(true,false), DRAWN(false,true)")
    void isClosingAndIsDrawnByStatus() {
        // Given
        DropSnapshot open = withStatus(SnapshotStatus.OPEN);
        DropSnapshot closing = withStatus(SnapshotStatus.CLOSING);
        DropSnapshot drawn = withStatus(SnapshotStatus.DRAWN);

        // When / Then
        assertThat(open.isClosing()).isFalse();
        assertThat(open.isDrawn()).isFalse();
        assertThat(closing.isClosing()).isTrue();
        assertThat(closing.isDrawn()).isFalse();
        assertThat(drawn.isClosing()).isFalse();
        assertThat(drawn.isDrawn()).isTrue();
    }
}
