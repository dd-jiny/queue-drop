package com.queuedrop.entry.application.port;

import com.queuedrop.entry.application.dto.DrawCompletedPayload;

/** draw.completed 발행. 실패 시 EventPublishException */
public interface DrawEventPublisher {

    void publishCompleted(DrawCompletedPayload payload);
}
