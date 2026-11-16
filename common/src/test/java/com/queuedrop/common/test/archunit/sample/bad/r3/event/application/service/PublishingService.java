package com.queuedrop.common.test.archunit.sample.bad.r3.event.application.service;

import com.queuedrop.common.event.EntryCreated;
import java.time.Instant;

public class PublishingService {
    Object build() { return new EntryCreated("id", 1L, 1L, 1, Instant.EPOCH); }
}
