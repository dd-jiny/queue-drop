package com.queuedrop.coupon.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.coupon.domain.model.CouponEvent;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

/** 쿠폰 이벤트 저장소 통합 테스트 (단계 문서 4장 T6-11 — 저장·조회·restore, V1 스키마). */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, CouponEventRepositoryImpl.class})
class CouponEventRepositoryImplTest {

    /** 마이크로초 정밀도 시각 */
    private static final Instant NOW = Instant.parse("2026-11-20T09:00:00.654321Z");
    private static final Instant START_AT = Instant.parse("2026-11-20T10:00:00.123456Z");

    @Autowired CouponEventRepositoryImpl repository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("[T6-11] save 는 id 가 부여된 이벤트를 돌려주고 나머지 필드는 그대로다")
    void saveAssignsId() {
        // Given
        CouponEvent event = CouponEvent.create("겨울 쿠폰", 100, START_AT, NOW);

        // When
        CouponEvent saved = repository.save(event);

        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getName()).isEqualTo("겨울 쿠폰");
        assertThat(saved.getTotalQuantity()).isEqualTo(100);
        assertThat(saved.getIssueStartAt()).isEqualTo(START_AT);
        assertThat(saved.getCreatedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("[T6-11] 저장 후 findById 로 읽으면 restore 된 이벤트의 모든 필드(마이크로초 Instant 포함)가 일치한다")
    void findByIdRoundTrip() {
        // Given
        long id = repository.save(CouponEvent.create("겨울 쿠폰", 7, START_AT, NOW)).getId();
        entityManager.flush();
        entityManager.clear();

        // When
        CouponEvent found = repository.findById(id).orElseThrow();

        // Then
        assertThat(found).usingRecursiveComparison()
                .isEqualTo(CouponEvent.restore(id, "겨울 쿠폰", 7, START_AT, NOW));
        assertThat(found.isStarted(START_AT)).isTrue();
    }

    @Test
    @DisplayName("[T6-11] 두 이벤트를 저장하면 id 가 서로 다르고, 없는 id 는 빈 값이다")
    void distinctIdsAndMissing() {
        // Given
        long first = repository.save(CouponEvent.create("A", 1, START_AT, NOW)).getId();
        long second = repository.save(CouponEvent.create("B", 2, START_AT, NOW)).getId();
        entityManager.flush();
        entityManager.clear();

        // When / Then
        assertThat(first).isNotEqualTo(second);
        assertThat(repository.findById(second).orElseThrow().getName()).isEqualTo("B");
        assertThat(repository.findById(Math.max(first, second) + 1_000)).isEmpty();
    }

    /** PostgreSQL 은 실패한 문장 이후 트랜잭션이 중단되므로 위반 INSERT 뒤에는 쿼리하지 않는다. */
    @Test
    @DisplayName("[T6-11] V1 스키마: total_quantity 0 은 CHECK 제약(ck_coupon_events_quantity)으로 거절된다")
    void quantityCheckConstraint() {
        // When / Then
        assertThatThrownBy(() -> jdbcTemplate.update(
                "insert into coupon_events (name, total_quantity, issue_start_at, created_at) values ('X', 0, ?, ?)",
                Timestamp.from(START_AT), Timestamp.from(NOW)))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_coupon_events_quantity");
    }
}
