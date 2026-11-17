package com.queuedrop.coupon.infrastructure.persistence;

import com.queuedrop.coupon.domain.model.IssuedCoupon;
import com.queuedrop.coupon.domain.repository.IssuedCouponRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
public class IssuedCouponRepositoryImpl implements IssuedCouponRepository {

    private final IssuedCouponJpaRepository jpa;

    @Override
    public boolean saveIfAbsent(IssuedCoupon coupon) {
        return jpa.insertIfAbsent(coupon.getCouponEventId(), coupon.getUserId(), coupon.getIssuedAt()) == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public List<IssuedCoupon> findByUserId(long userId) {
        return jpa.findByUserIdOrderByIssuedAtDesc(userId).stream().map(IssuedCouponMapper::toDomain).toList();
    }
}
