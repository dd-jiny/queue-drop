package com.queuedrop.coupon.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.coupon.domain.exception.DomainException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class CouponEventTest {

    private static final Instant NOW = Instant.parse("2026-11-17T11:00:00Z");
    private static final Instant START_AT = Instant.parse("2026-11-17T12:00:00Z");

    @Test
    @DisplayName("[T6-01] 정상 값으로 create 하면 id 는 null, 입력 값과 createdAt=now 를 갖는다")
    void createWithValidValues() {
        // Given / When
        CouponEvent event = CouponEvent.create("오픈 기념 쿠폰", 100, START_AT, NOW);

        // Then
        assertThat(event.getId()).isNull();
        assertThat(event.getName()).isEqualTo("오픈 기념 쿠폰");
        assertThat(event.getTotalQuantity()).isEqualTo(100);
        assertThat(event.getIssueStartAt()).isEqualTo(START_AT);
        assertThat(event.getCreatedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("[T6-01] 수량 1(경계)이면 생성된다")
    void createWithMinimumQuantity() {
        // Given / When
        CouponEvent event = CouponEvent.create("쿠폰", 1, START_AT, NOW);

        // Then
        assertThat(event.getTotalQuantity()).isEqualTo(1);
    }

    @ParameterizedTest(name = "totalQuantity={0}")
    @ValueSource(ints = {0, -1})
    @DisplayName("[T6-01] 수량이 1 미만이면 DomainException")
    void quantityBelowOneThrows(int totalQuantity) {
        // When / Then
        assertThatThrownBy(() -> CouponEvent.create("쿠폰", totalQuantity, START_AT, NOW))
                .isInstanceOf(DomainException.class);
    }

    @ParameterizedTest(name = "name=[{0}]")
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    @DisplayName("[T6-01] 이름이 null·빈 문자열·공백이면 DomainException")
    void blankNameThrows(String name) {
        // When / Then
        assertThatThrownBy(() -> CouponEvent.create(name, 100, START_AT, NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T6-01] 시작 시각이 과거이면 DomainException")
    void pastStartAtThrows() {
        // Given
        Instant past = NOW.minusSeconds(1);

        // When / Then
        assertThatThrownBy(() -> CouponEvent.create("쿠폰", 100, past, NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T6-01] 시작 시각이 now 와 같으면(경계) 미래가 아니므로 DomainException")
    void startAtEqualToNowThrows() {
        // When / Then
        assertThatThrownBy(() -> CouponEvent.create("쿠폰", 100, NOW, NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T6-01] 시작 시각이 null 이면 NPE 가 아니라 DomainException")
    void nullStartAtThrows() {
        // When / Then
        assertThatThrownBy(() -> CouponEvent.create("쿠폰", 100, null, NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T6-02] 시작 12:00 이면 11:59:59 에는 시작 전(false), 12:00 에는 시작(true)")
    void isStartedAtBoundary() {
        // Given
        CouponEvent event = CouponEvent.create("쿠폰", 100, START_AT, NOW);

        // When / Then
        assertThat(event.isStarted(Instant.parse("2026-11-17T11:59:59Z"))).isFalse();
        assertThat(event.isStarted(START_AT)).isTrue();
    }

    @Test
    @DisplayName("[T6-02] 시작 시각 이후에는 계속 true")
    void isStartedAfterStart() {
        // Given
        CouponEvent event = CouponEvent.create("쿠폰", 100, START_AT, NOW);

        // When / Then
        assertThat(event.isStarted(START_AT.plusSeconds(3600))).isTrue();
    }
}
