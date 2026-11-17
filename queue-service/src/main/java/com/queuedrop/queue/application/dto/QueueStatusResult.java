package com.queuedrop.queue.application.dto;

import com.queuedrop.queue.domain.model.QueueState;

/** state 는 String (presentation 이 domain enum 을 보지 않도록). nextPollAfterMs=0 이면 폴링 중단 */
public record QueueStatusResult(String state, Long aheadCount, long nextPollAfterMs) {

    public static QueueStatusResult waiting(long aheadCount, long nextPollAfterMs) {
        return new QueueStatusResult(QueueState.WAITING.name(), aheadCount, nextPollAfterMs);
    }

    public static QueueStatusResult admitted() {
        return new QueueStatusResult(QueueState.ADMITTED.name(), null, 0);
    }

    public static QueueStatusResult soldOut() {
        return new QueueStatusResult(QueueState.SOLD_OUT.name(), null, 0);
    }

    public static QueueStatusResult notInQueue() {
        return new QueueStatusResult(QueueState.NOT_IN_QUEUE.name(), null, 0);
    }
}
