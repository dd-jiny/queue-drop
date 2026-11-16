package com.queuedrop.common.test.archunit.sample.bad.r2.toinfra.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r2.toinfra.infrastructure.persistence.DropEntity;

public class LeakyService {
    private final DropEntity entity = new DropEntity();
}
