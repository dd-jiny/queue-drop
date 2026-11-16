package com.queuedrop.queue.application.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AdmissionPropertiesTest {

    @ParameterizedTest(name = "intervalMs={0} → gateWindowMs={1}")
    @CsvSource({
            "1000, 900",
            "10, 9",
            "2, 1",
            "1, 1"
    })
    @DisplayName("[T3-10] 게이트 window 는 주기의 90%(내림), 최소 1ms (E-31)")
    void gateWindowMs(long intervalMs, long expected) {
        // Given
        AdmissionProperties props = new AdmissionProperties(intervalMs, 100, 300);

        // When
        long window = props.gateWindowMs();

        // Then
        assertThat(window).isEqualTo(expected);
    }
}
