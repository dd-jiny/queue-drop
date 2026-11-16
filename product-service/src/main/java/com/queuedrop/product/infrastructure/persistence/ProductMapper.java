package com.queuedrop.product.infrastructure.persistence;

import com.queuedrop.product.domain.model.Product;
import org.springframework.stereotype.Component;

@Component
class ProductMapper {

    Product toDomain(ProductJpaEntity e) {
        return Product.restore(e.getId(), e.getName(), e.getDescription(), e.getImageUrl(), e.getCreatedAt());
    }

    ProductJpaEntity toEntity(Product p) {
        return new ProductJpaEntity(p.getId(), p.getName(), p.getDescription(), p.getImageUrl(), p.getCreatedAt());
    }
}
