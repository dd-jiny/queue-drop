package com.queuedrop.common.test.archunit.sample.bad.r5.infrastructure.persistence;

import com.queuedrop.common.test.archunit.sample.bad.r5.presentation.DropResponse;

public class ResponseAwareAdapter {
    public DropResponse load() { return new DropResponse(1L); }
}
