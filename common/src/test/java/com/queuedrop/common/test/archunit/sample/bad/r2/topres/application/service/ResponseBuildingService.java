package com.queuedrop.common.test.archunit.sample.bad.r2.topres.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r2.topres.presentation.DropResponse;

public class ResponseBuildingService {
    public Object get() { return new DropResponse(1L); }
}
