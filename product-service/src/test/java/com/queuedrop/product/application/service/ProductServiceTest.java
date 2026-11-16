package com.queuedrop.product.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.product.application.dto.ProductCreateCommand;
import com.queuedrop.product.application.dto.ProductResult;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.service.fake.FakeProductRepository;
import com.queuedrop.product.domain.exception.DomainException;
import com.queuedrop.product.domain.model.Product;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ProductServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-16T09:00:00Z");
    private static final Requester ADMIN = new Requester(1L, "ADMIN");
    private static final Requester USER = new Requester(2L, "USER");

    private FakeProductRepository productRepository;
    private ProductService productService;

    @BeforeEach
    void setUp() {
        productRepository = new FakeProductRepository();
        productService = new ProductService(productRepository, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("[T2-17] ADMIN 이 상품을 등록하면 저장되고 결과에 id 와 createdAt=now 가 담긴다")
    void adminCreatesProduct() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(ADMIN, "한정판 스니커즈", "설명", "https://img.example.com/a.png");

        // When
        ProductResult result = productService.create(command);

        // Then: 저장 상태
        assertThat(productRepository.findAll()).hasSize(1);
        Product saved = productRepository.findAll().getFirst();
        assertThat(saved.getName()).isEqualTo("한정판 스니커즈");
        assertThat(saved.getCreatedAt()).isEqualTo(NOW);

        // Then: 결과
        assertThat(result.id()).isNotNull().isEqualTo(saved.getId());
        assertThat(result.name()).isEqualTo("한정판 스니커즈");
        assertThat(result.description()).isEqualTo("설명");
        assertThat(result.imageUrl()).isEqualTo("https://img.example.com/a.png");
        assertThat(result.createdAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("[T2-17] USER 가 상품을 등록하면 COMMON_FORBIDDEN 이고 저장되지 않는다")
    void userIsForbidden() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(USER, "상품", null, null);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> productService.create(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
        assertThat(productRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T2-17] requester 가 null 이면 COMMON_FORBIDDEN 이고 저장되지 않는다")
    void nullRequesterIsForbidden() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(null, "상품", null, null);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> productService.create(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
        assertThat(productRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T2-17] role 이 null 인 requester 는 COMMON_FORBIDDEN")
    void nullRoleIsForbidden() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(new Requester(3L, null), "상품", null, null);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> productService.create(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
        assertThat(productRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T2-17] ADMIN 이라도 상품명이 공백이면 PRODUCT_INVALID_PRODUCT, cause 는 DomainException, 저장 없음")
    void blankNameIsInvalidProduct() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(ADMIN, "   ", null, null);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> productService.create(command))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(ProductErrorCode.INVALID_PRODUCT);
                    assertThat(e.getCause()).isInstanceOf(DomainException.class);
                });
        assertThat(productRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T2-17] 권한 검사가 입력 검증보다 먼저다 (USER + 빈 상품명 → COMMON_FORBIDDEN)")
    void authorizationBeforeValidation() {
        // Given
        ProductCreateCommand command = new ProductCreateCommand(USER, "", null, null);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> productService.create(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
    }
}
