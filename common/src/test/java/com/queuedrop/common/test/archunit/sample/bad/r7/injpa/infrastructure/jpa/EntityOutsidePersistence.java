package com.queuedrop.common.test.archunit.sample.bad.r7.injpa.infrastructure.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class EntityOutsidePersistence {
    @Id
    private Long id;
}
