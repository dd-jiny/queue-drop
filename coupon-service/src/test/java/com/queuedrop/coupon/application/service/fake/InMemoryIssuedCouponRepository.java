package com.queuedrop.coupon.application.service.fake;

import com.queuedrop.coupon.domain.model.IssuedCoupon;
import com.queuedrop.coupon.domain.repository.IssuedCouponRepository;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * (couponEventId, userId) 키 인메모리 저장소. 키가 있으면 false (ON CONFLICT DO NOTHING 과 같은 의미).
 * failSaveWith 로 지정한 예외는 이후 saveIfAbsent 가 계속 던진다 (DB 장애). findByUserId 는 최신순.
 */
public class InMemoryIssuedCouponRepository implements IssuedCouponRepository {

    private record Key(long couponEventId, long userId) {}

    private final Map<Key, IssuedCoupon> store = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();
    private RuntimeException saveFailure;
    private int saveCalls;

    @Override
    public synchronized boolean saveIfAbsent(IssuedCoupon coupon) {
        saveCalls++;
        if (saveFailure != null) {
            throw saveFailure;
        }
        Key key = new Key(coupon.getCouponEventId(), coupon.getUserId());
        if (store.containsKey(key)) {
            return false;
        }
        store.put(key, IssuedCoupon.restore(idSequence.incrementAndGet(), coupon.getCouponEventId(),
                coupon.getUserId(), coupon.getIssuedAt()));
        return true;
    }

    /** 최신순 (issuedAt 내림차순, 같으면 id 내림차순) */
    @Override
    public synchronized List<IssuedCoupon> findByUserId(long userId) {
        return store.values().stream()
                .filter(c -> c.getUserId() == userId)
                .sorted(Comparator.comparing(IssuedCoupon::getIssuedAt).thenComparing(IssuedCoupon::getId).reversed())
                .toList();
    }

    public synchronized void failSaveWith(RuntimeException failure) {
        this.saveFailure = failure;
    }

    public synchronized Optional<IssuedCoupon> find(long couponEventId, long userId) {
        return Optional.ofNullable(store.get(new Key(couponEventId, userId)));
    }

    public synchronized int saveCalls() {
        return saveCalls;
    }

    public synchronized int count() {
        return store.size();
    }
}
