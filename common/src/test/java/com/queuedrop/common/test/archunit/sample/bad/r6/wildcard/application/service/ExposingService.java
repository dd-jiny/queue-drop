package com.queuedrop.common.test.archunit.sample.bad.r6.wildcard.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.wildcard.domain.model.Drop;
import java.util.List;

public class ExposingService {
    public void saveAll(List<? extends Drop> drops) {}
}
