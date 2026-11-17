package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.port.ApplicantCounter;
import com.queuedrop.entry.application.port.GateUnavailableException;
import java.util.HashMap;
import java.util.Map;

/**
 * Redis 응모자 수(SCARD) 대역. 설정하지 않은 드로우는 0.
 * failNext() 후 다음 count 1건, failAlways() 후 모든 count 가 GateUnavailableException 을 던진다.
 */
public class FakeApplicantCounter implements ApplicantCounter {

    private final Map<Long, Long> counts = new HashMap<>();
    private boolean failNext;
    private boolean failAlways;
    private int calls;

    @Override
    public long count(long dropId) {
        calls++;
        if (failNext || failAlways) {
            failNext = false;
            throw new GateUnavailableException("fake count 실패", new RuntimeException("simulated redis down"));
        }
        return counts.getOrDefault(dropId, 0L);
    }

    public void set(long dropId, long count) {
        counts.put(dropId, count);
    }

    public void failNext() {
        this.failNext = true;
    }

    public void failAlways() {
        this.failAlways = true;
    }

    public int calls() {
        return calls;
    }
}
