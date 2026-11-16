package com.queuedrop.product.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

interface ProductJpaRepository extends JpaRepository<ProductJpaEntity, Long> {
}
