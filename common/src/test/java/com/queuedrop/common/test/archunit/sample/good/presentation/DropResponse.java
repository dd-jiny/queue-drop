package com.queuedrop.common.test.archunit.sample.good.presentation;

import com.queuedrop.common.test.archunit.sample.good.application.dto.DropResult;

public record DropResponse(Long id, int capacity) {
    public static DropResponse from(DropResult result) { return new DropResponse(result.id(), result.capacity()); }
}
