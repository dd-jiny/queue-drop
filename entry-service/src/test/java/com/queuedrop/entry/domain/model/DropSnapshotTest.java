package com.queuedrop.entry.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.domain.exception.DomainException;
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
}
