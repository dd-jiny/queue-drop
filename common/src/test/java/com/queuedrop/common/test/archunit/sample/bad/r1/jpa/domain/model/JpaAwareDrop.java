package com.queuedrop.common.test.archunit.sample.bad.r1.jpa.domain.model;

import jakarta.persistence.Transient;

public class JpaAwareDrop {
    @Transient
    private int cache;
}
