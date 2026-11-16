package com.queuedrop.common.test.archunit.sample.bad.r1.toupper.domain.model;

import com.queuedrop.common.test.archunit.sample.bad.r1.toupper.application.dto.SomeResult;

public class DropKnowsResult {
    public SomeResult toResult() { return new SomeResult(1L); }
}
