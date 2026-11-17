package com.queuedrop.entry.presentation.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.queuedrop.entry.application.service.DrawService;
import java.time.Clock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** entry.draw.scheduler-enabled 에 따른 DrawScheduler 빈 등록 여부 (단계 문서 5단계 v1.11, queue AdmissionScheduler 와 같은 방식) */
class DrawSchedulerConditionTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(DrawService.class, () -> mock(DrawService.class))
            .withBean(Clock.class, Clock::systemUTC)
            .withUserConfiguration(DrawScheduler.class);

    @Test
    @DisplayName("[T5-21] scheduler-enabled=false 면 DrawScheduler 빈을 만들지 않는다")
    void disabled() {
        // Given / When / Then
        runner.withPropertyValues("entry.draw.scheduler-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(DrawScheduler.class));
    }

    @Test
    @DisplayName("[T5-21] scheduler-enabled=true 면 DrawScheduler 빈이 있다")
    void enabled() {
        // Given / When / Then
        runner.withPropertyValues("entry.draw.scheduler-enabled=true")
                .run(context -> assertThat(context).hasSingleBean(DrawScheduler.class));
    }

    @Test
    @DisplayName("[T5-21] 속성이 없으면 기본으로 DrawScheduler 빈이 있다 (matchIfMissing)")
    void enabledByDefault() {
        // Given / When / Then
        runner.run(context -> assertThat(context).hasSingleBean(DrawScheduler.class));
    }
}
