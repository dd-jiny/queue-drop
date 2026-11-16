package com.queuedrop.queue.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import com.queuedrop.queue.application.config.AdmissionProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** queue.admission.* 검증 (단계 문서 3.3 설정 클래스: 배치 크기 1~1000, 주기·TTL 1 이상) */
class SchedulingConfigTest {

    private final SchedulingConfig config = new SchedulingConfig();

    @ParameterizedTest(name = "batchSize={0}")
    @CsvSource({"1", "1000"})
    @DisplayName("[T3-15] 배치 크기 경계 1·1000 은 허용된다")
    void batchSizeBoundariesAccepted(int batchSize) {
        // When
        AdmissionProperties props = config.admissionProperties(1_000, batchSize, 300);

        // Then
        assertThat(props).isEqualTo(new AdmissionProperties(1_000, batchSize, 300));
    }

    @ParameterizedTest(name = "intervalMs={0}, batchSize={1}, ttl={2}")
    @CsvSource({
            "1000, 0, 300",
            "1000, 1001, 300",
            "0, 100, 300",
            "1000, 100, 0"
    })
    @DisplayName("[T3-15] 배치 크기 0·1001, 주기 0, TTL 0 이면 기동 실패(IllegalStateException)")
    void invalidPropertiesRejected(long intervalMs, int batchSize, long ttl) {
        // When / Then
        assertThatIllegalStateException()
                .isThrownBy(() -> config.admissionProperties(intervalMs, batchSize, ttl));
    }

    @Test
    @DisplayName("[T3-15] application.yml 기본값(1000, 100, 300)은 유효하다")
    void defaultsAreValid() {
        // When
        AdmissionProperties props = config.admissionProperties(1_000, 100, 300);

        // Then
        assertThat(props.batchSize()).isEqualTo(100);
    }
}
