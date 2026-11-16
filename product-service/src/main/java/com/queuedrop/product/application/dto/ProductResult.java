package com.queuedrop.product.application.dto;

import com.queuedrop.product.domain.model.Product;

import java.time.Instant;

public record ProductResult(Long id, String name, String description, String imageUrl, Instant createdAt) {
    public static ProductResult from(Product p) {
        return new ProductResult(p.getId(), p.getName(), p.getDescription(), p.getImageUrl(), p.getCreatedAt());
    }
}
