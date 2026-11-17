package com.queuedrop.entry.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.domain.exception.DomainException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EntryTest {

    private static final long DROP_ID = 10L;
    private static final long USER_ID = 100L;
    private static final Instant APPLIED_AT = Instant.parse("2026-11-16T10:30:00Z");

    @Test
    @DisplayName("[T4-01] 정상 값으로 apply 하면 status=APPLIED, id=null 이고 입력 값을 그대로 갖는다")
    void applyWithValidValues() {
        // Given
        int entrySeq = 7;

        // When
        Entry entry = Entry.apply(DROP_ID, USER_ID, entrySeq, APPLIED_AT);

        // Then
        assertThat(entry.getStatus()).isEqualTo(EntryStatus.APPLIED);
        assertThat(entry.getId()).isNull();
        assertThat(entry.getDropId()).isEqualTo(DROP_ID);
        assertThat(entry.getUserId()).isEqualTo(USER_ID);
        assertThat(entry.getEntrySeq()).isEqualTo(entrySeq);
        assertThat(entry.getAppliedAt()).isEqualTo(APPLIED_AT);
    }

    @Test
    @DisplayName("[T4-01] entrySeq=1(최소 경계)이면 생성된다")
    void applyWithMinimumSeq() {
        // Given / When
        Entry entry = Entry.apply(DROP_ID, USER_ID, 1, APPLIED_AT);

        // Then
        assertThat(entry.getEntrySeq()).isEqualTo(1);
    }

    @Test
    @DisplayName("[T4-01] entrySeq=0 이면 DomainException")
    void applyWithZeroSeqThrows() {
        // Given
        int entrySeq = 0;

        // When / Then
        assertThatThrownBy(() -> Entry.apply(DROP_ID, USER_ID, entrySeq, APPLIED_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-01] entrySeq 가 음수이면 DomainException")
    void applyWithNegativeSeqThrows() {
        // When / Then
        assertThatThrownBy(() -> Entry.apply(DROP_ID, USER_ID, -1, APPLIED_AT))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T4-01] appliedAt 이 null 이면 NullPointerException (단계 문서 시그니처: requireNonNull)")
    void applyWithNullAppliedAtThrows() {
        // When / Then
        assertThatThrownBy(() -> Entry.apply(DROP_ID, USER_ID, 1, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("[T4-01] restore 는 id 와 상태를 포함한 저장 값을 그대로 복원한다")
    void restorePreservesValues() {
        // Given / When
        Entry entry = Entry.restore(5L, DROP_ID, USER_ID, 3, EntryStatus.APPLIED, APPLIED_AT);

        // Then
        assertThat(entry.getId()).isEqualTo(5L);
        assertThat(entry.getDropId()).isEqualTo(DROP_ID);
        assertThat(entry.getUserId()).isEqualTo(USER_ID);
        assertThat(entry.getEntrySeq()).isEqualTo(3);
        assertThat(entry.getStatus()).isEqualTo(EntryStatus.APPLIED);
        assertThat(entry.getAppliedAt()).isEqualTo(APPLIED_AT);
    }
}
