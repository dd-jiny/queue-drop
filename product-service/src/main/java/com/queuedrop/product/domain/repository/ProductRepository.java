package com.queuedrop.product.domain.repository;

import com.queuedrop.product.domain.model.Product;

import java.util.Optional;

public interface ProductRepository {
    Product save(Product product);

    Optional<Product> findById(Long id);

    boolean existsById(Long id);
}
