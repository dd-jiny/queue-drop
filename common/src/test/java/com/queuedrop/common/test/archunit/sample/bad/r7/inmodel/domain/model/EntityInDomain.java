package com.queuedrop.common.test.archunit.sample.bad.r7.inmodel.domain.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class EntityInDomain {
    @Id
    private Long id;
}
