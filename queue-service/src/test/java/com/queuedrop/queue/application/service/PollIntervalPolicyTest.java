package com.queuedrop.queue.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PollIntervalPolicyTest {

    private final PollIntervalPolicy policy = new PollIntervalPolicy();

    @ParameterizedTest(name = "aheadCount={0} → {1}ms")
    @CsvSource({
            "0, 1000",
            "99, 1000",
            "100, 3000",
            "999, 3000",
            "1000, 5000",
            "1000000, 5000"
    })
    @DisplayName("[T3-09] 앞 인원 0·99 → 1초, 100·999 → 3초, 1000 이상 → 5초")
    void nextPollAfterMs(long aheadCount, long expectedMs) {
        // When
        long next = policy.nextPollAfterMs(aheadCount);

        // Then
        assertThat(next).isEqualTo(expectedMs);
    }
}
