package com.queuedrop.coupon.application.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.queuedrop.coupon.domain.model.IssueOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CouponErrorCodeTest {

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource({
            "NOT_FOUND, EVENT_NOT_FOUND",
            "NOT_STARTED, NOT_STARTED",
            "DUPLICATE, DUPLICATE",
            "SOLD_OUT, SOLD_OUT"
    })
    @DisplayName("[T6-30] from(IssueOutcome) 은 거절 4가지를 에러 코드로 바꾼다 (NOT_FOUND → EVENT_NOT_FOUND)")
    void fromOutcome(IssueOutcome outcome, CouponErrorCode expected) {
        // When / Then
        assertThat(CouponErrorCode.from(outcome)).isEqualTo(expected);
    }

    @Test
    @DisplayName("[T6-30] from(OK) 는 IllegalArgumentException")
    void fromOkRejected() {
        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> CouponErrorCode.from(IssueOutcome.OK));
    }

    @Test
    @DisplayName("[T6-30] from(NOT_FOUND) 는 404 다 (Redis meta 없음 = 미등록 또는 초기화 실패)")
    void notFoundIs404() {
        // When
        CouponErrorCode code = CouponErrorCode.from(IssueOutcome.NOT_FOUND);

        // Then
        assertThat(code.code()).isEqualTo("COUPON_EVENT_NOT_FOUND");
        assertThat(code.status()).isEqualTo(404);
    }

    @ParameterizedTest(name = "{0} → {1} / {2}")
    @CsvSource({
            "EVENT_NOT_FOUND, COUPON_EVENT_NOT_FOUND, 404",
            "NOT_STARTED, COUPON_NOT_STARTED, 409",
            "DUPLICATE, COUPON_DUPLICATE, 409",
            "SOLD_OUT, COUPON_SOLD_OUT, 409",
            "TEMPORARILY_FAILED, COUPON_TEMPORARILY_FAILED, 503",
            "INVALID_EVENT, COUPON_INVALID_EVENT, 400"
    })
    @DisplayName("[T6-30] 각 코드는 COUPON_* 코드명과 HTTP 상태를 가지고 메시지가 비어 있지 않다")
    void codeAndStatus(CouponErrorCode errorCode, String expectedCode, int expectedStatus) {
        // When / Then
        assertThat(errorCode.code()).isEqualTo(expectedCode);
        assertThat(errorCode.status()).isEqualTo(expectedStatus);
        assertThat(errorCode.message()).isNotBlank();
    }

    @Test
    @DisplayName("[T6-30] 에러 코드는 표의 6가지뿐이다")
    void exactlySixCodes() {
        // Then
        assertThat(CouponErrorCode.values()).hasSize(6);
    }
}
