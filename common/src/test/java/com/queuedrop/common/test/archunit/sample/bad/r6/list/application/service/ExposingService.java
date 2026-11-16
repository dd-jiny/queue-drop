package com.queuedrop.common.test.archunit.sample.bad.r6.list.application.service;

import com.queuedrop.common.test.archunit.sample.bad.r6.list.domain.model.Drop;
import java.util.List;

public class ExposingService {
    public List<Drop> getAll() { return null; }
}
