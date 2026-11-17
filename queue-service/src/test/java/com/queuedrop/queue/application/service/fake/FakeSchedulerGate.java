package com.queuedrop.queue.application.service.fake;

import com.queuedrop.queue.application.port.SchedulerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** 통과 여부를 테스트가 정한다. 요청된 (name, window) 를 기록한다. */
public class FakeSchedulerGate implements SchedulerGate {

    public record Pass(String name, Duration window) {}

    private boolean open = true;
    private final List<Pass> requests = new ArrayList<>();

    @Override
    public boolean tryPass(String name, Duration window) {
        requests.add(new Pass(name, window));
        return open;
    }

    public void close() {
        this.open = false;
    }

    public List<Pass> requests() {
        return List.copyOf(requests);
    }
}
