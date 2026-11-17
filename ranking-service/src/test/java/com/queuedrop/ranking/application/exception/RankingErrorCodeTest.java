package com.queuedrop.ranking.application.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RankingErrorCodeTest {

    @ParameterizedTest(name = "{0} → {1} / {2}")
    @CsvSource({
            "INVALID_SIZE, RANKING_INVALID_SIZE, 400",
            "INVALID_WEEK, RANKING_INVALID_WEEK, 400",
            "TEMPORARILY_FAILED, RANKING_TEMPORARILY_FAILED, 503"
    })
    @DisplayName("[T7-11·T7-27 보강] 각 코드는 RANKING_* 코드명과 HTTP 상태를 가지고 메시지가 비어 있지 않다")
    void codeAndStatus(RankingErrorCode errorCode, String expectedCode, int expectedStatus) {
        // When / Then
        assertThat(errorCode.code()).isEqualTo(expectedCode);
        assertThat(errorCode.status()).isEqualTo(expectedStatus);
        assertThat(errorCode.message()).isNotBlank();
    }

    @Test
    @DisplayName("[T7-11·T7-27 보강] 에러 코드는 3가지뿐이다")
    void exactlyThreeCodes() {
        // Then
        assertThat(RankingErrorCode.values()).hasSize(3);
    }
}
