package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.port.DrawEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import java.util.ArrayList;
import java.util.List;

/**
 * draw.completed 발행 대역. failNext() 후 다음 발행 1건, failAlways() 후 모든 발행이 EventPublishException 으로 실패하고
 * published 에는 기록되지 않는다. attempted 는 실패한 시도까지 모두 기록한다.
 */
public class FakeDrawEventPublisher implements DrawEventPublisher {

    private final List<DrawCompletedPayload> published = new ArrayList<>();
    private final List<DrawCompletedPayload> attempted = new ArrayList<>();
    private boolean failNext;
    private boolean failAlways;

    @Override
    public void publishCompleted(DrawCompletedPayload payload) {
        attempted.add(payload);
        if (failNext || failAlways) {
            failNext = false;
            throw new EventPublishException("draw.completed 발행 실패 (fake)",
                    new RuntimeException("simulated timeout"));
        }
        published.add(payload);
    }

    public void failNext() {
        this.failNext = true;
    }

    public void failAlways() {
        this.failAlways = true;
    }

    /** failAlways 를 해제한다 (브로커 복구) */
    public void recover() {
        this.failAlways = false;
        this.failNext = false;
    }

    public List<DrawCompletedPayload> attempted() {
        return List.copyOf(attempted);
    }

    public List<DrawCompletedPayload> published() {
        return List.copyOf(published);
    }
}
