package com.queuedrop.queue.presentation.scheduler;

import com.queuedrop.queue.application.service.AdmissionService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 시간이 보내는 요청을 받는 입력 어댑터. 테스트는 scheduler-enabled=false 로 끄고 admit() 을 직접 호출한다 */
@Component
@ConditionalOnProperty(name = "queue.admission.scheduler-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class AdmissionScheduler {

    private final AdmissionService admissionService;

    @Scheduled(fixedRateString = "${queue.admission.interval-ms}")
    public void run() {
        admissionService.admit();
    }
}
