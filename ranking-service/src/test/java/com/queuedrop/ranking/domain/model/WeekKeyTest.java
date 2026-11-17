package com.queuedrop.ranking.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.ranking.domain.exception.DomainException;
import com.queuedrop.ranking.domain.exception.InvalidWeekKeyException;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WeekKeyTest {

    private static Instant kst(String localDateTime) {
        return OffsetDateTime.parse(localDateTime + "+09:00").toInstant();
    }

    @Test
    @DisplayName("[T7-01] KST 일요일 23:59:59 는 그 주(2026-W47)에 속한다")
    void sundayLastSecondBelongsToSameWeek() {
        // Given
        Instant sunday = kst("2026-11-22T23:59:59");

        // When
        WeekKey week = WeekKey.of(sunday);

        // Then
        assertThat(week).isEqualTo(new WeekKey(2026, 47));
        assertThat(week.value()).isEqualTo("2026-W47");
    }

    @Test
    @DisplayName("[T7-02] KST 월요일 00:00:00 은 다음 주(2026-W48)가 시작된다")
    void mondayMidnightStartsNextWeek() {
        // Given
        Instant monday = kst("2026-11-23T00:00:00");

        // When
        WeekKey week = WeekKey.of(monday);

        // Then
        assertThat(week).isEqualTo(new WeekKey(2026, 48));
        assertThat(week.value()).isEqualTo("2026-W48");
    }

    @Test
    @DisplayName("[T7-03] UTC 일요일 15:00 은 KST 월요일 00:00 이므로 UTC 기준(W47)이 아닌 2026-W48")
    void usesSeoulZoneNotUtc() {
        // Given
        Instant utcSunday = Instant.parse("2026-11-22T15:00:00Z");

        // When
        WeekKey week = WeekKey.of(utcSunday);

        // Then
        assertThat(week).isEqualTo(new WeekKey(2026, 48));
        assertThat(week.value()).isNotEqualTo("2026-W47").isEqualTo("2026-W48");
    }

    @Test
    @DisplayName("[T7-04] 2027-01-01 KST 는 주 기준 연도가 달력 연도와 달라 2026-W53")
    void weekBasedYearDiffersFromCalendarYear() {
        // Given
        Instant newYear = kst("2027-01-01T00:00:00");

        // When
        WeekKey week = WeekKey.of(newYear);

        // Then
        assertThat(week.weekBasedYear()).isEqualTo(2026);
        assertThat(week.week()).isEqualTo(53);
        assertThat(week.value()).isEqualTo("2026-W53");
    }

    @Test
    @DisplayName("[T7-05] 53주가 있는 해의 \"2026-W53\" 은 파싱된다")
    void parseWeek53InLongYear() {
        // When
        WeekKey week = WeekKey.parse("2026-W53");

        // Then
        assertThat(week).isEqualTo(new WeekKey(2026, 53));
    }

    @Test
    @DisplayName("[T7-05] \"2026-W01\" (최소 주차 경계)은 파싱된다")
    void parseWeek01() {
        // When
        WeekKey week = WeekKey.parse("2026-W01");

        // Then
        assertThat(week).isEqualTo(new WeekKey(2026, 1));
    }

    @ParameterizedTest(name = "value=[{0}]")
    @ValueSource(strings = {"2025-W53", "2026-W54", "2026-W00", "2026-W99", "2026-47", "2026-W7", "abc", "", " 2026-W47", "2026-W47 "})
    @DisplayName("[T7-05] 형식이 틀리거나 그 해에 없는 주차면 InvalidWeekKeyException (DomainException)")
    void parseInvalidThrows(String value) {
        // When / Then
        assertThatThrownBy(() -> WeekKey.parse(value))
                .isInstanceOf(InvalidWeekKeyException.class)
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T7-05] 생성자로 그 해에 없는 주차(2025-W53, 2026-W0)를 만들면 InvalidWeekKeyException")
    void constructorValidatesWeekRange() {
        // When / Then
        assertThatThrownBy(() -> new WeekKey(2025, 53)).isInstanceOf(InvalidWeekKeyException.class);
        assertThatThrownBy(() -> new WeekKey(2026, 0)).isInstanceOf(InvalidWeekKeyException.class);
    }

    @ParameterizedTest(name = "value={0}")
    @ValueSource(strings = {"2026-W05", "2026-W47", "2026-W53", "2027-W01"})
    @DisplayName("[T7-06] parse 후 value() 는 원래 문자열과 같다 (왕복 일치, 0 채움 유지)")
    void parseValueRoundTrip(String value) {
        // When
        String roundTrip = WeekKey.parse(value).value();

        // Then
        assertThat(roundTrip).isEqualTo(value);
    }
}
