package com.queuedrop.common.test.archunit.sample.modeling.application.service;

import com.queuedrop.common.test.archunit.sample.modeling.domain.modeling.DropSnapshot;

import java.util.List;

public class SnapshotService {
    public DropSnapshot get() { return new DropSnapshot(1L); }

    public List<DropSnapshot> getAll(DropSnapshot seed) { return List.of(seed); }
}
