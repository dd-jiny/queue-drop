package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import java.util.ArrayList;
import java.util.List;

/**
 * 발행된 payload 를 기록한다. failNext() 후 다음 발행 1건은 EventPublishException 으로 실패하고 published 에는 기록되지 않는다.
 * attempted 는 실패한 시도까지 모두 기록한다 (v1.5: 실패한 발행의 eventId 대조용).
 */
public class FakeEntryEventPublisher implements EntryEventPublisher {

    private final List<EntryCreatedPayload> published = new ArrayList<>();
    private final List<EntryCreatedPayload> attempted = new ArrayList<>();
    private boolean failNext;

    @Override
    public void publishCreated(EntryCreatedPayload payload) {
        attempted.add(payload);
        if (failNext) {
            failNext = false;
            throw new EventPublishException("브로커 기록 확인 시간 초과 (fake)",
                    new RuntimeException("simulated timeout"));
        }
        published.add(payload);
    }

    public void failNext() {
        this.failNext = true;
    }

    public List<EntryCreatedPayload> attempted() {
        return List.copyOf(attempted);
    }

    public List<EntryCreatedPayload> published() {
        return List.copyOf(published);
    }
}
