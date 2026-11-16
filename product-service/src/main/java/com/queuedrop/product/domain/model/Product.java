package com.queuedrop.product.domain.model;

import com.queuedrop.product.domain.exception.DomainException;
import lombok.Getter;

import java.time.Instant;

/** 상품. 드로우는 상품을 ID 로만 참조한다. */
@Getter
public class Product {

    private final Long id;              // 저장 전 null
    private final String name;
    private final String description;
    private final String imageUrl;
    private final Instant createdAt;

    private Product(Long id, String name, String description, String imageUrl, Instant createdAt) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.imageUrl = imageUrl;
        this.createdAt = createdAt;
    }

    public static Product create(String name, String description, String imageUrl, Instant now) {
        if (name == null || name.isBlank()) {
            throw new DomainException("상품명은 필수입니다");
        }
        return new Product(null, name, description, imageUrl, now);
    }

    /** 영속 계층 복원 전용 (Mapper 에서만 사용). 검증하지 않는다. */
    public static Product restore(Long id, String name, String description, String imageUrl, Instant createdAt) {
        return new Product(id, name, description, imageUrl, createdAt);
    }
}
