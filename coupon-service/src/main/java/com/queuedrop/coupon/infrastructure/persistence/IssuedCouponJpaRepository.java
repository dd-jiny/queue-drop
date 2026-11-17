package com.queuedrop.coupon.infrastructure.persistence;

import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

interface IssuedCouponJpaRepository extends JpaRepository<IssuedCouponJpaEntity, Long> {

    /**
     * UK(coupon_event_id, user_id) 충돌은 예외 대신 무시한다 (트랜잭션이 rollback-only 가 되지 않는다).
     * @return 1 = 저장, 0 = 이미 발급 저장됨
     */
    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO issued_coupons (coupon_event_id, user_id, issued_at)
            VALUES (:eventId, :userId, :issuedAt)
            ON CONFLICT (coupon_event_id, user_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("eventId") Long eventId, @Param("userId") Long userId,
                       @Param("issuedAt") Instant issuedAt);

    List<IssuedCouponJpaEntity> findByUserIdOrderByIssuedAtDesc(Long userId);
}
