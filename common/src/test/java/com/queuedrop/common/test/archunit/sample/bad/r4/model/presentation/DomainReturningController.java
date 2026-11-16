package com.queuedrop.common.test.archunit.sample.bad.r4.model.presentation;

import com.queuedrop.common.test.archunit.sample.bad.r4.model.domain.model.Drop;

public class DomainReturningController {
    public Drop get() { return new Drop(); }
}
