package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.application.service.fake.InMemoryDrawResultRepository;
import com.queuedrop.entry.domain.model.DrawResult;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DrawQueryServiceTest {

    private static final Instant DRAWN_AT = Instant.parse("2026-11-17T12:00:05Z");

    private InMemoryDrawResultRepository results;
    private DrawQueryService service;

    @BeforeEach
    void setUp() {
        results = new InMemoryDrawResultRepository();
        service = new DrawQueryService(results);
    }

    @Test
    @DisplayName("[T5-23] (application) 추첨 기록이 있으면 dropId·응모자 수·당첨 수·시드(음수 포함)·추첨 시각을 돌려준다")
    void summary() {
        // Given
        results.save(DrawResult.of(7L, -123L, 9, 3, DRAWN_AT));

        // When
        DrawSummaryResult summary = service.summary(7L);

        // Then
        assertThat(summary).isEqualTo(new DrawSummaryResult(7L, 9, 3, -123L, DRAWN_AT));
    }

    @Test
    @DisplayName("[T5-23] (application) 추첨 전이면 409 ENTRY_DRAW_NOT_COMPLETED")
    void notCompleted() {
        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.summary(7L))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(DrawErrorCode.NOT_COMPLETED));
    }
}
