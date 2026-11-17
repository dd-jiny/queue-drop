package com.queuedrop.coupon.application.port;

public interface CouponEventPublisher {

    /**
     * 브로커 ack 까지 동기 대기한다.
     * @throws EventPublishException 정해진 시간 안에 기록이 확인되지 않음
     */
    void publishIssued(CouponIssuedPayload payload);
}
