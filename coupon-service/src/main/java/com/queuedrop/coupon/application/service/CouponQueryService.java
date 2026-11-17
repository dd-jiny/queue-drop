package com.queuedrop.coupon.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.MyCouponResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.model.CouponEvent;
import com.queuedrop.coupon.domain.model.IssuedCoupon;
import com.queuedrop.coupon.domain.repository.CouponEventRepository;
import com.queuedrop.coupon.domain.repository.IssuedCouponRepository;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CouponQueryService {

    private final CouponEventRepository eventRepository;
    private final IssuedCouponRepository issuedRepository;
    private final CouponGate gate;

    /** DB 이벤트 + Redis 잔여 수량. Redis 장애면 503 TEMPORARILY_FAILED. 트랜잭션 없음 — DB 커넥션을 쥔 채 Redis 를 호출하지 않는다 */
    public CouponEventResult event(long couponEventId) {
        CouponEvent event = eventRepository.findById(couponEventId)
                .orElseThrow(() -> new ApplicationException(CouponErrorCode.EVENT_NOT_FOUND));
        long remaining;
        try {
            remaining = gate.remaining(couponEventId);
        } catch (GateUnavailableException e) {
            throw new ApplicationException(CouponErrorCode.TEMPORARILY_FAILED, e);
        }
        return new CouponEventResult(event.getId(), event.getName(), event.getTotalQuantity(),
                remaining, event.getIssueStartAt());
    }

    /** 내 쿠폰 목록, 최신 발급순 */
    @Transactional(readOnly = true)
    public List<MyCouponResult> myCoupons(Long userId) {
        List<IssuedCoupon> coupons = issuedRepository.findByUserId(userId);
        Map<Long, String> names = new HashMap<>();
        return coupons.stream()
                .sorted(Comparator.comparing(IssuedCoupon::getIssuedAt).reversed())
                .map(c -> new MyCouponResult(c.getCouponEventId(),
                        names.computeIfAbsent(c.getCouponEventId(), this::eventName),
                        c.getIssuedAt()))
                .toList();
    }

    private String eventName(Long couponEventId) {
        Optional<CouponEvent> event = eventRepository.findById(couponEventId);
        return event.map(CouponEvent::getName).orElse(null);
    }
}
