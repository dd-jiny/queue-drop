package com.queuedrop.entry.application.port;

public interface EntryEventPublisher {

    /**
     * 브로커 기록이 확인될 때까지 기다린다 (동기).
     * @throws EventPublishException 정해진 시간 안에 기록이 확인되지 않음
     */
    void publishCreated(EntryCreatedPayload payload);
}
