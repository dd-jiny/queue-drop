package com.queuedrop.common.test.archunit.sample.good.presentation;

import com.queuedrop.common.test.archunit.sample.good.application.service.DropService;
import java.util.List;

public class DropController {
    private final DropService dropService;

    public DropController(DropService dropService) { this.dropService = dropService; }

    public DropResponse get(Long id) { return DropResponse.from(dropService.get(id)); }

    public List<DropResponse> getAll() { return dropService.getAll().stream().map(DropResponse::from).toList(); }
}
