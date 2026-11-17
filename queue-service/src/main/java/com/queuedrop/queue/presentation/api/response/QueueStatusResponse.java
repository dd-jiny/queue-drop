package com.queuedrop.queue.presentation.api.response;

import com.queuedrop.queue.application.dto.QueueStatusResult;

public record QueueStatusResponse(String state, Long aheadCount, long nextPollAfterMs) {

    public static QueueStatusResponse from(QueueStatusResult r) {
        return new QueueStatusResponse(r.state(), r.aheadCount(), r.nextPollAfterMs());
    }
}
