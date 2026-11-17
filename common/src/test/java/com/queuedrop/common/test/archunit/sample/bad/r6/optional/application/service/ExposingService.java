package com.queuedrop.common.test.archunit.sample.bad.r6.optional.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.optional.domain.model.Drop;
import java.util.Optional;

public class ExposingService {
    public Optional<Drop> find() { return Optional.empty(); }
}
