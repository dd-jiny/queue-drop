package com.queuedrop.coupon.application.service.fake;

import com.queuedrop.coupon.application.port.CouponEventPublisher;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 발행된 payload 를 기록한다. failNext() 후 다음 발행 1건은 EventPublishException 으로 실패하고 published 에는 남지 않는다.
 * attempted 는 실패한 시도까지 모두 기록한다. beforeFailure 를 지정하면 실패를 던지기 직전에 실행한다
 * (예: "브로커에 기록돼 컨슈머가 저장 확정까지 했지만 ack 는 실패" 순서 재현).
 */
public class FakeCouponEventPublisher implements CouponEventPublisher {

    private final List<CouponIssuedPayload> published = new ArrayList<>();
    private final List<CouponIssuedPayload> attempted = new ArrayList<>();
    private boolean failNext;
    private Consumer<CouponIssuedPayload> beforeFailure = p -> { };
    private EventPublishException lastFailure;

    @Override
    public void publishIssued(CouponIssuedPayload payload) {
        attempted.add(payload);
        if (failNext) {
            failNext = false;
            beforeFailure.accept(payload);
            lastFailure = new EventPublishException("브로커 기록 확인 시간 초과 (fake)",
                    new RuntimeException("simulated timeout"));
            throw lastFailure;
        }
        published.add(payload);
    }

    public void failNext() {
        this.failNext = true;
    }

    /** 다음 발행 1건을 실패시키되, 예외 직전에 action 을 실행한다 */
    public void failNextAfter(Consumer<CouponIssuedPayload> action) {
        this.failNext = true;
        this.beforeFailure = action;
    }

    public List<CouponIssuedPayload> attempted() {
        return List.copyOf(attempted);
    }

    public List<CouponIssuedPayload> published() {
        return List.copyOf(published);
    }

    public EventPublishException lastFailure() {
        return lastFailure;
    }
}
