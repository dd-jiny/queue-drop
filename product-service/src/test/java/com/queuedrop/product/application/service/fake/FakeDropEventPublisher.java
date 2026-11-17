package com.queuedrop.product.application.service.fake;

import com.queuedrop.product.application.port.DropCreatedPayload;
import com.queuedrop.product.application.port.DropEventPublisher;
import java.util.ArrayList;
import java.util.List;

/** 발행 요청된 payload 를 순서대로 기록한다. */
public class FakeDropEventPublisher implements DropEventPublisher {

    private final List<DropCreatedPayload> created = new ArrayList<>();

    @Override
    public void publishCreated(DropCreatedPayload payload) {
        created.add(payload);
    }

    public List<DropCreatedPayload> created() {
        return List.copyOf(created);
    }
}
