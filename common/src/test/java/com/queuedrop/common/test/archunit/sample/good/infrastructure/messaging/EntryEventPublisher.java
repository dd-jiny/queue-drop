package com.queuedrop.common.test.archunit.sample.good.infrastructure.messaging;

import com.queuedrop.common.event.EntryCreated;
import java.time.Instant;

/** 메시지 계약(common.event)은 infrastructure 에서만 다룬다 */
public class EntryEventPublisher {
    public EntryCreated toMessage(Long dropId, Long userId, int seq) {
        return new EntryCreated("id", dropId, userId, seq, Instant.EPOCH);
    }
}
