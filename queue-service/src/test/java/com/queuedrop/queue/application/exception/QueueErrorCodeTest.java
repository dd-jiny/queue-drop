package com.queuedrop.queue.application.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class QueueErrorCodeTest {

    @ParameterizedTest(name = "{0} → {1} / {2}")
    @CsvSource({
            "DROP_NOT_FOUND, QUEUE_DROP_NOT_FOUND, 404",
            "NOT_OPEN, QUEUE_NOT_OPEN, 409",
            "CLOSED, QUEUE_CLOSED, 409",
            "SOLD_OUT, QUEUE_SOLD_OUT, 409"
    })
    @DisplayName("[T3-03] 에러 코드는 QUEUE_* 코드와 HTTP 상태(404/409)를 가진다 (0단계 에러 계약)")
    void codeAndStatus(QueueErrorCode errorCode, String expectedCode, int expectedStatus) {
        // When / Then
        assertThat(errorCode.code()).isEqualTo(expectedCode);
        assertThat(errorCode.status()).isEqualTo(expectedStatus);
        assertThat(errorCode.message()).isNotBlank();
    }
}
