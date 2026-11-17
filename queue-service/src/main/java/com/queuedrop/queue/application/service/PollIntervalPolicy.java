package com.queuedrop.queue.application.service;

import org.springframework.stereotype.Component;

/** 다음 폴링 간격. 순번이 가까울수록 짧게 (queue-enter.md 3장) */
@Component
public class PollIntervalPolicy {

    public long nextPollAfterMs(long aheadCount) {
        if (aheadCount < 100) return 1_000;
        if (aheadCount < 1_000) return 3_000;
        return 5_000;
    }
}
