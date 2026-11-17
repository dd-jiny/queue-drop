package com.queuedrop.product.support;

import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;
import com.queuedrop.product.domain.repository.DropRepository;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * 실제 DropRepository(dropRepositoryImpl)에 위임하면서, 지정한 dropId 의 findById 에서 예외를 던지고
 * dropId 별 findById 호출 수(= 리스너 처리 시도 수)를 센다. 리스너 재시도·DLT 분류 검증용.
 * presentation 테스트가 domain 타입을 import 하지 않도록 support 패키지에 둔다.
 */
public class FaultInjectingDropRepository implements DropRepository {

    private final DropRepository delegate;
    private final Map<Long, Supplier<RuntimeException>> faults = new ConcurrentHashMap<>();
    private final Map<Long, AtomicInteger> findCalls = new ConcurrentHashMap<>();

    public FaultInjectingDropRepository(DropRepository delegate) {
        this.delegate = delegate;
    }

    public void failOnFind(Long dropId, Supplier<RuntimeException> fault) {
        faults.put(dropId, fault);
    }

    public int findCalls(Long dropId) {
        AtomicInteger count = findCalls.get(dropId);
        return count == null ? 0 : count.get();
    }

    public void reset() {
        faults.clear();
        findCalls.clear();
    }

    @Override
    public Drop save(Drop drop) {
        return delegate.save(drop);
    }

    @Override
    public Optional<Drop> findById(Long id) {
        findCalls.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        Supplier<RuntimeException> fault = faults.get(id);
        if (fault != null) {
            throw fault.get();
        }
        return delegate.findById(id);
    }

    @Override
    public DropPage search(DropStatus status, Instant now, int page, int size) {
        return delegate.search(status, now, page, size);
    }

    /** 운영 어댑터(빈 이름 dropRepositoryImpl)를 감싸 @Primary 로 등록한다. */
    @TestConfiguration(proxyBeanMethods = false)
    public static class Config {
        @Bean
        @Primary
        FaultInjectingDropRepository faultInjectingDropRepository(
                @Qualifier("dropRepositoryImpl") DropRepository delegate) {
            return new FaultInjectingDropRepository(delegate);
        }
    }
}
