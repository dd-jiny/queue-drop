package com.queuedrop.common.test.archunit.sample.bad.r4.entity.presentation;

import com.queuedrop.common.test.archunit.sample.bad.r4.entity.infrastructure.persistence.DropEntity;

public class EntityReturningController {
    public DropEntity get() { return new DropEntity(); }
}
