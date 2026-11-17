package com.queuedrop.product.presentation.api.response;

import com.queuedrop.product.application.dto.ProductResult;

import java.time.Instant;

public record ProductResponse(Long id, String name, String description, String imageUrl, Instant createdAt) {
    public static ProductResponse from(ProductResult r) {
        return new ProductResponse(r.id(), r.name(), r.description(), r.imageUrl(), r.createdAt());
    }
}
