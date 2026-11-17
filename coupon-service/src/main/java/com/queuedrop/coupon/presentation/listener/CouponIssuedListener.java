package com.queuedrop.coupon.presentation.listener;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.coupon.application.dto.CouponSaveCommand;
import com.queuedrop.coupon.application.service.CouponSaveService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** coupon.issued → DB 저장. 실패는 예외로 올려 DefaultErrorHandler(3회 재시도 후 DLT)에 맡긴다 */
@Component
@RequiredArgsConstructor
public class CouponIssuedListener {

    private final CouponSaveService saveService;

    @KafkaListener(topics = Topics.COUPON_ISSUED)   // groupId 는 공통 설정(${spring.application.name})
    public void on(CouponIssued message) {
        // message.eventId() = 요청 ID (claim 판정 기준)
        saveService.save(new CouponSaveCommand(message.eventId(), message.couponEventId(), message.userId(),
                message.issuedAt()));
    }
}
