package com.queuedrop.common.test.archunit.sample.bad.r7.inservice.application.service;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class EntityInApplication {
    @Id
    private Long id;
}
