package com.queuedrop.coupon.application.service.fake;

import com.queuedrop.coupon.domain.model.CouponEvent;
import com.queuedrop.coupon.domain.repository.CouponEventRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** 저장 시 id 를 1부터 부여한다. findById 호출은 기록한다 (권한 검사가 조회보다 먼저인지 확인용). */
public class InMemoryCouponEventRepository implements CouponEventRepository {

    private final Map<Long, CouponEvent> store = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();
    private final List<Long> findByIdCalls = new ArrayList<>();
    private int saveCalls;

    @Override
    public synchronized CouponEvent save(CouponEvent event) {
        saveCalls++;
        long id = event.getId() != null ? event.getId() : idSequence.incrementAndGet();
        CouponEvent saved = CouponEvent.restore(id, event.getName(), event.getTotalQuantity(),
                event.getIssueStartAt(), event.getCreatedAt());
        store.put(id, saved);
        return saved;
    }

    @Override
    public synchronized Optional<CouponEvent> findById(long id) {
        findByIdCalls.add(id);
        return Optional.ofNullable(store.get(id));
    }

    /** 테스트 준비용: findById 기록 없이 직접 넣는다 */
    public synchronized CouponEvent given(CouponEvent event) {
        CouponEvent saved = save(event);
        saveCalls--;
        return saved;
    }

    public synchronized List<CouponEvent> all() {
        return List.copyOf(store.values());
    }

    public synchronized List<Long> findByIdCalls() {
        return List.copyOf(findByIdCalls);
    }

    public synchronized int saveCalls() {
        return saveCalls;
    }

    public synchronized int count() {
        return store.size();
    }
}
