package com.queuedrop.product.infrastructure.persistence;

import com.queuedrop.product.domain.model.Product;
import com.queuedrop.product.domain.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
class ProductRepositoryImpl implements ProductRepository {

    private final ProductJpaRepository jpa;
    private final ProductMapper mapper;

    @Override
    public Product save(Product product) {
        return mapper.toDomain(jpa.save(mapper.toEntity(product)));
    }

    @Override
    public Optional<Product> findById(Long id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public boolean existsById(Long id) {
        return jpa.existsById(id);
    }
}
