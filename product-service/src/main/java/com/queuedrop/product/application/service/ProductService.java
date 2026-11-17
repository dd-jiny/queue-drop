package com.queuedrop.product.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.product.application.dto.ProductCreateCommand;
import com.queuedrop.product.application.dto.ProductResult;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.domain.exception.DomainException;
import com.queuedrop.product.domain.model.Product;
import com.queuedrop.product.domain.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/** 상품 등록 유스케이스 */
@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final Clock clock;

    @Transactional
    public ProductResult create(ProductCreateCommand command) {
        AdminGuard.requireAdmin(command.requester());
        Product product;
        try {
            product = Product.create(command.name(), command.description(), command.imageUrl(), clock.instant());
        } catch (DomainException e) {
            throw new ApplicationException(ProductErrorCode.INVALID_PRODUCT, e);
        }
        return ProductResult.from(productRepository.save(product));
    }
}
