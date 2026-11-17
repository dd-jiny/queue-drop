package com.queuedrop.common.test.archunit.sample.good.domain.model;

import com.queuedrop.common.test.archunit.sample.good.domain.exception.DomainException;

/** domain 은 domain 과 java.* 에만 의존한다 */
public class Drop {
    private final Long id;
    private final int capacity;

    public Drop(Long id, int capacity) {
        if (capacity <= 0) throw new DomainException("capacity must be positive");
        this.id = id;
        this.capacity = capacity;
    }

    public Long getId() { return id; }

    public int getCapacity() { return capacity; }
}
