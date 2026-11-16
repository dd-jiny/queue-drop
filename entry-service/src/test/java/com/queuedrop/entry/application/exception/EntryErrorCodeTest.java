package com.queuedrop.entry.application.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.queuedrop.entry.domain.model.ApplyOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EntryErrorCodeTest {

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource({
            "CLOSED, CLOSED",
            "NO_TOKEN, NO_TOKEN",
            "DUPLICATE, DUPLICATE",
            "SOLD_OUT, SOLD_OUT"
    })
    @DisplayName("[T4-35] from(ApplyOutcome) 은 거절 4가지를 같은 사유의 에러 코드로 바꾼다")
    void fromOutcome(ApplyOutcome outcome, EntryErrorCode expected) {
        // When / Then
        assertThat(EntryErrorCode.from(outcome)).isEqualTo(expected);
    }

    @Test
    @DisplayName("[T4-35] from(OK) 는 IllegalArgumentException")
    void fromOkRejected() {
        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> EntryErrorCode.from(ApplyOutcome.OK));
    }

    @ParameterizedTest(name = "{0} → {1} / {2}")
    @CsvSource({
            "CLOSED, ENTRY_CLOSED, 409",
            "NO_TOKEN, ENTRY_NO_TOKEN, 409",
            "DUPLICATE, ENTRY_DUPLICATE, 409",
            "SOLD_OUT, ENTRY_SOLD_OUT, 409",
            "TEMPORARILY_FAILED, ENTRY_TEMPORARILY_FAILED, 503",
            "NOT_FOUND, ENTRY_NOT_FOUND, 404"
    })
    @DisplayName("[T4-35] 각 코드는 ENTRY_* 코드와 entry-apply.md 3장 표의 HTTP 상태를 가진다")
    void codeAndStatus(EntryErrorCode errorCode, String expectedCode, int expectedStatus) {
        // When / Then
        assertThat(errorCode.code()).isEqualTo(expectedCode);
        assertThat(errorCode.status()).isEqualTo(expectedStatus);
        assertThat(errorCode.message()).isNotBlank();
    }

    @Test
    @DisplayName("[T4-35] 에러 코드는 표의 6가지뿐이다")
    void exactlySixCodes() {
        // Then
        assertThat(EntryErrorCode.values()).hasSize(6);
    }
}
