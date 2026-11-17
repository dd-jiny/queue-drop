package com.queuedrop.entry.presentation.scheduler;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import com.queuedrop.entry.application.service.DrawService;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * DrawScheduler 단위 (단계 문서 5단계 3.4). DrawService 는 구체 클래스라 Fake 대신 Mockito 로 호출 순서만 확인한다.
 * 마감(close) 이 먼저여야 같은 주기에 방금 마감한 드로우를 추첨 대상으로 볼 수 있고, 재발행은 추첨 뒤 미발행분을 줍는다.
 */
class DrawSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-11-20T12:00:00Z");

    @Test
    @DisplayName("[T5-21] run 은 같은 Clock 시각으로 closeDueDrops → drawReadyDrops → republishUnpublished 순서로 호출한다")
    void runsInOrder() {
        // Given
        DrawService drawService = mock(DrawService.class);
        DrawScheduler scheduler = new DrawScheduler(drawService, Clock.fixed(NOW, ZoneOffset.UTC));

        // When
        scheduler.run();

        // Then
        InOrder order = inOrder(drawService);
        order.verify(drawService).closeDueDrops(NOW);
        order.verify(drawService).drawReadyDrops(NOW);
        order.verify(drawService).republishUnpublished();
        order.verifyNoMoreInteractions();
    }
}
