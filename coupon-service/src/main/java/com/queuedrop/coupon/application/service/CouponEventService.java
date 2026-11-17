package com.queuedrop.coupon.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponEventRegisterCommand;
import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.CouponReinitializeCommand;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.exception.DomainException;
import com.queuedrop.coupon.domain.model.CouponEvent;
import com.queuedrop.coupon.domain.repository.CouponEventRepository;
import java.time.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 쿠폰 이벤트 등록 · Redis 재초기화.
 * 트랜잭션을 열지 않는다: INSERT 1건은 그 자체로 원자적이고, Redis 초기화는 저장(커밋) 후에 한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CouponEventService {

    private final CouponEventRepository eventRepository;
    private final CouponGate gate;
    private final Clock clock;

    public CouponEventResult register(CouponEventRegisterCommand command) {
        AdminGuard.requireAdmin(command.requester());
        CouponEvent event;
        try {
            event = CouponEvent.create(command.name(), command.totalQuantity(), command.issueStartAt(),
                    clock.instant());
        } catch (DomainException e) {
            throw new ApplicationException(CouponErrorCode.INVALID_EVENT, e);
        }
        CouponEvent saved = eventRepository.save(event);
        try {
            gate.initialize(saved.getId(), saved.getTotalQuantity(), saved.getIssueStartAt());
        } catch (GateUnavailableException e) {
            log.warn("coupon redis init failed couponEventId={} — reinitialize required", saved.getId(), e);
        }
        // remaining 을 Redis 에서 다시 읽지 않는다: 막 등록한 이벤트의 잔여는 totalQuantity 다
        // (초기화 실패 시에도 응답은 등록 성공 — 재초기화로 복구)
        return new CouponEventResult(saved.getId(), saved.getName(), saved.getTotalQuantity(),
                saved.getTotalQuantity(), saved.getIssueStartAt());
    }

    /** SET NX 라 진행 중인 잔여 수량을 덮어쓰지 않는다. Redis 장애면 503 TEMPORARILY_FAILED */
    public void reinitialize(CouponReinitializeCommand command) {
        AdminGuard.requireAdmin(command.requester());
        CouponEvent event = eventRepository.findById(command.couponEventId())
                .orElseThrow(() -> new ApplicationException(CouponErrorCode.EVENT_NOT_FOUND));
        try {
            gate.initialize(event.getId(), event.getTotalQuantity(), event.getIssueStartAt());
        } catch (GateUnavailableException e) {
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
    }
}
