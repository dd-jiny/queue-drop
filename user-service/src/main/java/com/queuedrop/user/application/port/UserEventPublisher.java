package com.queuedrop.user.application.port;

public interface UserEventPublisher {
    /** 트랜잭션 커밋 후 발행된다 (구현체 책임). 커밋 전 롤백되면 발행하지 않는다. */
    void publishRegistered(UserRegisteredPayload payload);
}
