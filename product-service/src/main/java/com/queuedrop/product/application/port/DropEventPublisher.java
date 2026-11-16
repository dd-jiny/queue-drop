package com.queuedrop.product.application.port;

public interface DropEventPublisher {
    /** 트랜잭션 안에서 호출되면 커밋 후 발행, 트랜잭션 밖이면 즉시 발행한다 (구현체 책임). */
    void publishCreated(DropCreatedPayload payload);
}
