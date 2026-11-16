package com.queuedrop.product.presentation.api.response;

import com.queuedrop.product.application.dto.DropPageResult;

import java.util.List;

public record DropPageResponse(List<DropResponse> content, long totalElements, int page, int size) {
    public static DropPageResponse from(DropPageResult r) {
        return new DropPageResponse(r.content().stream().map(DropResponse::from).toList(),
                r.totalElements(), r.page(), r.size());
    }
}
