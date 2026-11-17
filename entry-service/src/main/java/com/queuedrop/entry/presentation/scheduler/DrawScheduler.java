package com.queuedrop.entry.presentation.scheduler;

import com.queuedrop.entry.application.service.DrawService;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 마감 → 추첨 → 미발행 재발행 순으로 매 주기 실행한다.
 * fixedDelay: 이전 실행이 끝난 뒤 간격을 두어 한 인스턴스 안에서 겹치지 않는다. 인스턴스 간 중복은 조건부 UPDATE 가 막는다
 */
@Component
@ConditionalOnProperty(name = "entry.draw.scheduler-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class DrawScheduler {

    private final DrawService drawService;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${entry.draw.interval-ms}")
    public void run() {
        Instant now = clock.instant();
        drawService.closeDueDrops(now);
        drawService.drawReadyDrops(now);
        drawService.republishUnpublished();
    }
}
