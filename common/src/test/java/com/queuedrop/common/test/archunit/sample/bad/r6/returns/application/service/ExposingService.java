package com.queuedrop.common.test.archunit.sample.bad.r6.returns.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.returns.domain.model.Drop;

public class ExposingService {
    public Drop get() { return null; }
}
