package com.queuedrop.product.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.product.domain.exception.DomainException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class ProductTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");

    @ParameterizedTest(name = "[T2-10] 상품명 \"{0}\"")
    @NullSource
    @ValueSource(strings = {"", "   "})
    @DisplayName("[T2-10] 상품명이 null, 빈 문자열, 공백만이면 DomainException")
    void rejectsBlankName(String name) {
        // Given: name

        // When / Then
        assertThatThrownBy(() -> Product.create(name, "설명", "https://img.example.com/a.png", NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T2-10] 상품명이 있으면 생성되고 id 는 null, createdAt 은 now 이다")
    void createsProduct() {
        // Given
        String name = "한정판 스니커즈";

        // When
        Product product = Product.create(name, "설명", "https://img.example.com/a.png", NOW);

        // Then
        assertThat(product.getId()).isNull();
        assertThat(product.getName()).isEqualTo(name);
        assertThat(product.getDescription()).isEqualTo("설명");
        assertThat(product.getImageUrl()).isEqualTo("https://img.example.com/a.png");
        assertThat(product.getCreatedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("[T2-10] 설명과 이미지 URL 은 선택값이다 (null 허용)")
    void allowsNullOptionalFields() {
        // Given / When
        Product product = Product.create("상품", null, null, NOW);

        // Then
        assertThat(product.getDescription()).isNull();
        assertThat(product.getImageUrl()).isNull();
    }

    @Test
    @DisplayName("[T2-10] restore 는 저장된 값을 그대로 복원한다")
    void restoresProduct() {
        // Given
        Instant createdAt = Instant.parse("2026-11-01T00:00:00Z");

        // When
        Product product = Product.restore(7L, "상품", "설명", "https://img.example.com/a.png", createdAt);

        // Then
        assertThat(product.getId()).isEqualTo(7L);
        assertThat(product.getName()).isEqualTo("상품");
        assertThat(product.getDescription()).isEqualTo("설명");
        assertThat(product.getImageUrl()).isEqualTo("https://img.example.com/a.png");
        assertThat(product.getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    @DisplayName("[T2-10] 상품명 100자, 이미지 URL 500자는 생성된다 (경계)")
    void acceptsMaxLengths() {
        // Given
        String name = "가".repeat(100);
        String imageUrl = "https://img.example.com/" + "a".repeat(500 - "https://img.example.com/".length());

        // When
        Product product = Product.create(name, null, imageUrl, NOW);

        // Then
        assertThat(product.getName()).isEqualTo(name);
        assertThat(product.getImageUrl()).hasSize(500);
    }

    @Test
    @DisplayName("[T2-10] 상품명이 101자면 DomainException")
    void rejectsTooLongName() {
        // Given
        String name = "가".repeat(101);

        // When / Then
        assertThatThrownBy(() -> Product.create(name, null, null, NOW))
                .isInstanceOf(DomainException.class);
    }

    @Test
    @DisplayName("[T2-10] 이미지 URL 이 501자면 DomainException")
    void rejectsTooLongImageUrl() {
        // Given
        String imageUrl = "https://img.example.com/" + "a".repeat(501 - "https://img.example.com/".length());

        // When / Then
        assertThatThrownBy(() -> Product.create("상품", null, imageUrl, NOW))
                .isInstanceOf(DomainException.class);
    }
}
