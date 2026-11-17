package com.queuedrop.entry.domain.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.domain.exception.DomainException;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DrawerTest {

    private final Drawer drawer = new Drawer();

    /** entry_seq 오름차순으로 정렬된 응모자 목록을 흉내 낸다 (userId 1001 ~ 1000+n). */
    private static List<Long> applicants(int n) {
        return LongStream.rangeClosed(1, n).map(i -> 1000L + i).boxed().toList();
    }

    @Test
    @DisplayName("[T5-01] 응모자 10명, K=3, seed=42 로 두 번 추첨하면 결과가 같다")
    void sameSeedSameResult() {
        // Given
        List<Long> ordered = applicants(10);

        // When
        Set<Long> first = drawer.draw(ordered, 3, 42L);
        Set<Long> second = drawer.draw(ordered, 3, 42L);

        // Then
        assertThat(first).hasSize(3);
        assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("[T5-02] 같은 목록에 seed 42 와 43 으로 추첨하면 결과가 다르다")
    void differentSeedDifferentResult() {
        // Given
        List<Long> ordered = applicants(10);

        // When
        Set<Long> seed42 = drawer.draw(ordered, 3, 42L);
        Set<Long> seed43 = drawer.draw(ordered, 3, 43L);

        // Then
        assertThat(seed42).hasSize(3);
        assertThat(seed43).hasSize(3);
        assertThat(seed43).isNotEqualTo(seed42);
    }

    @Test
    @DisplayName("[T5-03] 응모자 3명, K=5 이면 3명 전원 당첨")
    void winnerCountGreaterThanApplicantsAllWin() {
        // Given
        List<Long> ordered = applicants(3);

        // When
        Set<Long> winners = drawer.draw(ordered, 5, 42L);

        // Then
        assertThat(winners).containsExactlyInAnyOrderElementsOf(ordered);
    }

    @Test
    @DisplayName("[T5-03] 응모자 수 == K(경계)이면 전원 당첨")
    void winnerCountEqualToApplicantsAllWin() {
        // Given
        List<Long> ordered = applicants(5);

        // When
        Set<Long> winners = drawer.draw(ordered, 5, 42L);

        // Then
        assertThat(winners).containsExactlyInAnyOrderElementsOf(ordered);
    }

    @Test
    @DisplayName("[T5-04] 응모자 0명, K=5 이면 빈 집합")
    void noApplicantsEmptyResult() {
        // Given
        List<Long> ordered = List.of();

        // When
        Set<Long> winners = drawer.draw(ordered, 5, 42L);

        // Then
        assertThat(winners).isEmpty();
    }

    @Test
    @DisplayName("[T5-05] 응모자 100명, K=10 이면 크기 10, 모두 입력에 포함, 입력 리스트는 바뀌지 않는다")
    void resultSubsetOfInputAndInputUnchanged() {
        // Given
        List<Long> ordered = new java.util.ArrayList<>(applicants(100));   // 가변 리스트로 넘겨 변경 여부 확인
        List<Long> snapshot = List.copyOf(ordered);

        // When
        Set<Long> winners = drawer.draw(ordered, 10, 42L);

        // Then
        assertThat(winners).hasSize(10);
        assertThat(ordered).containsAll(winners);
        assertThat(ordered).containsExactlyElementsOf(snapshot);
    }

    @Test
    @DisplayName("[T5-06] K=0 이면 DomainException")
    void zeroWinnerCountThrows() {
        // When / Then
        assertThatThrownBy(() -> drawer.draw(applicants(10), 0, 42L))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T5-06] K 가 음수이면 DomainException")
    void negativeWinnerCountThrows() {
        // When / Then
        assertThatThrownBy(() -> drawer.draw(applicants(10), -1, 42L))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T5-01] 서로 다른 Drawer 인스턴스로 추첨해도 같은 입력·시드면 결과가 같다 (상태 없음, 재현성)")
    void reproducibleAcrossInstances() {
        // Given
        List<Long> ordered = applicants(50);

        // When
        Set<Long> first = new Drawer().draw(ordered, 7, 123456789L);
        Set<Long> second = new Drawer().draw(List.copyOf(ordered), 7, 123456789L);

        // Then
        assertThat(first).hasSize(7);
        assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("[T5-03] K=1(최소 경계), 응모자 1명이면 그 1명이 당첨")
    void singleApplicantSingleWinner() {
        // Given
        List<Long> ordered = applicants(1);

        // When
        Set<Long> winners = drawer.draw(ordered, 1, 42L);

        // Then
        assertThat(winners).containsExactly(1001L);
    }

    @Test
    @DisplayName("[T5-05] 반환된 당첨자 집합은 수정할 수 없다")
    void resultIsUnmodifiable() {
        // Given
        Set<Long> winners = drawer.draw(applicants(10), 3, 42L);

        // When / Then
        assertThatThrownBy(() -> winners.add(9999L))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
