package com.queuedrop.coupon.application.service;

import com.queuedrop.coupon.application.dto.CouponSaveCommand;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.domain.model.IssuedCoupon;
import com.queuedrop.coupon.domain.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * coupon.issued 처리. @Transactional 을 걸지 않는다 — saveIfAbsent 가 자체 트랜잭션을 가지며,
 * 바깥 트랜잭션이 있으면 제약 위반 시 rollback-only 로 UnexpectedRollbackException 이 난다.
 * 저장 실패 예외는 그대로 올린다 (DefaultErrorHandler 재시도 후 DLT).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CouponSaveService {

    private final IssuedCouponRepository issuedRepository;
    private final CouponGate gate;

    public void save(CouponSaveCommand command) {
        // ① 저장 확정: 보상된 발급 또는 이전 시도의 유령 이벤트면 false. true 이후 보상은 거절된다 (E-40)
        //    없으면 보상으로 되돌아간 수량을 다른 사람이 받아 초과 발급이 된다
        if (!gate.claimForSave(command.couponEventId(), command.userId(), command.eventId())) {
            log.warn("유령 발급 이벤트 무시 couponEventId={} userId={} requestId={}",
                    command.couponEventId(), command.userId(), command.eventId());
            return;
        }
        // ② 같은 사용자 쿠폰이 이미 있으면 무시 (재전달, 멱등)
        boolean saved = issuedRepository.saveIfAbsent(
                IssuedCoupon.issue(command.couponEventId(), command.userId(), command.issuedAt()));
        if (!saved) {
            log.debug("이미 저장된 쿠폰 couponEventId={} userId={}", command.couponEventId(), command.userId());
        }
    }
}
