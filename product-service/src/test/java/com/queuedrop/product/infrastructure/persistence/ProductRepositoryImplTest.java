package com.queuedrop.product.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.product.domain.model.Product;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, ProductRepositoryImpl.class, ProductMapper.class})
class ProductRepositoryImplTest {

    /** PostgreSQL timestamptz 는 마이크로초 정밀도. 마이크로초 단위까지 왕복되는지 본다. */
    private static final Instant CREATED_AT = Instant.parse("2026-11-16T10:00:00.123456Z");

    @Autowired ProductRepositoryImpl productRepository;
    @Autowired TestEntityManager entityManager;

    @Test
    @DisplayName("[T2-18] 상품 save 후 findById 로 다시 읽으면 id 가 부여되고 모든 필드(마이크로초 Instant 포함)가 일치한다")
    void saveAndFindRoundTrip() {
        // Given
        Product saved = productRepository.save(
                Product.create("한정판 스니커즈", "설명입니다", "https://img.example.com/a.png", CREATED_AT));
        entityManager.clear();   // 1차 캐시가 아니라 DB 에서 다시 읽게 한다

        // When
        Product found = productRepository.findById(saved.getId()).orElseThrow();

        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(found.getId()).isEqualTo(saved.getId());
        assertThat(found.getName()).isEqualTo("한정판 스니커즈");
        assertThat(found.getDescription()).isEqualTo("설명입니다");
        assertThat(found.getImageUrl()).isEqualTo("https://img.example.com/a.png");
        assertThat(found.getCreatedAt()).isEqualTo(CREATED_AT);
    }

    @Test
    @DisplayName("[T2-18] 설명·이미지 URL 이 null 인 상품도 왕복된다")
    void nullableFieldsRoundTrip() {
        // Given
        Product saved = productRepository.save(Product.create("상품", null, null, CREATED_AT));
        entityManager.clear();

        // When
        Product found = productRepository.findById(saved.getId()).orElseThrow();

        // Then
        assertThat(found.getDescription()).isNull();
        assertThat(found.getImageUrl()).isNull();
    }

    @Test
    @DisplayName("[T2-18] existsById 는 저장된 id 에만 true, findById 는 없는 id 에 빈 값")
    void existsAndFindMissing() {
        // Given
        Product saved = productRepository.save(Product.create("상품", null, null, CREATED_AT));
        entityManager.clear();

        // When / Then
        assertThat(productRepository.existsById(saved.getId())).isTrue();
        assertThat(productRepository.existsById(saved.getId() + 1000)).isFalse();
        assertThat(productRepository.findById(saved.getId() + 1000)).isEmpty();
    }
}
