package com.queuedrop.common.test.archunit.sample.good.infrastructure.persistence;

import com.queuedrop.common.test.archunit.sample.good.domain.model.Drop;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class DropEntity {
    @Id
    private Long id;
    private int capacity;

    protected DropEntity() {}

    public DropEntity(Long id, int capacity) { this.id = id; this.capacity = capacity; }

    public Drop toDomain() { return new Drop(id, capacity); }
}
