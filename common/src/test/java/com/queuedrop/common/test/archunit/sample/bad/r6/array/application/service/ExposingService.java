package com.queuedrop.common.test.archunit.sample.bad.r6.array.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.array.domain.model.Drop;

public class ExposingService {
    public Drop[] toArray() { return new Drop[0]; }
}
