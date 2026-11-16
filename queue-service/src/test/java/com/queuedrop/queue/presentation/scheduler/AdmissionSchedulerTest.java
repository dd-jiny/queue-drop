package com.queuedrop.queue.presentation.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.queuedrop.queue.application.service.AdmissionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.annotation.Scheduled;

/** queue.admission.scheduler-enabled 에 따른 빈 등록 여부 (단계 문서 3.4, v1.2) */
class AdmissionSchedulerTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(AdmissionService.class, () -> mock(AdmissionService.class))
            .withUserConfiguration(AdmissionScheduler.class);

    @Test
    @DisplayName("[T3-22] scheduler-enabled=false 면 AdmissionScheduler 빈을 만들지 않는다")
    void disabled() {
        runner.withPropertyValues("queue.admission.scheduler-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(AdmissionScheduler.class));
    }

    @Test
    @DisplayName("[T3-22] scheduler-enabled=true 면 AdmissionScheduler 빈이 있다")
    void enabled() {
        runner.withPropertyValues("queue.admission.scheduler-enabled=true")
                .run(context -> assertThat(context).hasSingleBean(AdmissionScheduler.class));
    }

    @Test
    @DisplayName("[T3-22] 속성이 없으면 기본으로 AdmissionScheduler 빈이 있다 (matchIfMissing)")
    void enabledByDefault() {
        runner.run(context -> assertThat(context).hasSingleBean(AdmissionScheduler.class));
    }

    @Test
    @DisplayName("[T3-22] run() 은 AdmissionService.admit() 에 위임한다")
    void runDelegatesToAdmit() {
        // Given
        AdmissionService admissionService = mock(AdmissionService.class);
        AdmissionScheduler scheduler = new AdmissionScheduler(admissionService);

        // When
        scheduler.run();

        // Then
        verify(admissionService).admit();
    }

    @Test
    @DisplayName("[T3-22] run() 은 queue.admission.interval-ms 주기로 실행된다 (@Scheduled fixedRateString)")
    void scheduledWithIntervalProperty() throws Exception {
        // When
        Scheduled scheduled = AdmissionScheduler.class.getMethod("run").getAnnotation(Scheduled.class);

        // Then
        assertThat(scheduled).isNotNull();
        assertThat(scheduled.fixedRateString()).isEqualTo("${queue.admission.interval-ms}");
    }
}
