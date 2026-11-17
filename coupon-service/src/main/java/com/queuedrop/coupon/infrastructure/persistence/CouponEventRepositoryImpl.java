package com.queuedrop.coupon.infrastructure.persistence;

import com.queuedrop.coupon.domain.model.CouponEvent;
import com.queuedrop.coupon.domain.repository.CouponEventRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
public class CouponEventRepositoryImpl implements CouponEventRepository {

    private final CouponEventJpaRepository jpa;

    @Override
    @Transactional
    public CouponEvent save(CouponEvent event) {
        return CouponEventMapper.toDomain(jpa.save(CouponEventMapper.toEntity(event)));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<CouponEvent> findById(long id) {
        return jpa.findById(id).map(CouponEventMapper::toDomain);
    }
}
