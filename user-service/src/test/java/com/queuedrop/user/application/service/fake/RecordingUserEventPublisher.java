package com.queuedrop.user.application.service.fake;

import com.queuedrop.user.application.port.UserEventPublisher;
import com.queuedrop.user.application.port.UserRegisteredPayload;
import java.util.ArrayList;
import java.util.List;

/** 발행 요청된 payload 를 순서대로 기록한다. */
public class RecordingUserEventPublisher implements UserEventPublisher {

    private final List<UserRegisteredPayload> registered = new ArrayList<>();

    @Override
    public void publishRegistered(UserRegisteredPayload payload) {
        registered.add(payload);
    }

    public void clear() {
        registered.clear();
    }

    public List<UserRegisteredPayload> registered() {
        return List.copyOf(registered);
    }
}
