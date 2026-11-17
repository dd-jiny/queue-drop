package com.queuedrop.entry.application.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DrawErrorCodeTest {

    @ParameterizedTest(name = "{0} → {1} / {2}")
    @CsvSource({
            "DROP_NOT_FOUND, ENTRY_DRAW_DROP_NOT_FOUND, 404",
            "NOT_COMPLETED, ENTRY_DRAW_NOT_COMPLETED, 409",
            "NOT_CLOSING, ENTRY_DRAW_NOT_CLOSING, 409",
            "ALREADY_DRAWN, ENTRY_DRAW_ALREADY_DRAWN, 409"
    })
    @DisplayName("[T5-29] 각 코드는 ENTRY_DRAW_* 코드와 단계 문서 3.2 의 HTTP 상태를 가진다")
    void codeAndStatus(DrawErrorCode errorCode, String expectedCode, int expectedStatus) {
        // When / Then
        assertThat(errorCode.code()).isEqualTo(expectedCode);
        assertThat(errorCode.status()).isEqualTo(expectedStatus);
        assertThat(errorCode.message()).isNotBlank();
    }
}
