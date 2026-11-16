package com.queuedrop.common.test.archunit.sample.bad.r6.nested.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.nested.domain.model.Drop;
import java.util.List;
import java.util.Map;

public class ExposingService {
    public Map<Long, List<Drop>> grouped() { return null; }
}
