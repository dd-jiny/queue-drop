package com.queuedrop.coupon.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.coupon.domain.model.IssuedCoupon;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 발급 쿠폰 저장소 통합 테스트 (단계 문서 4장 T6-11). V2 의 UK(coupon_event_id, user_id) + ON CONFLICT DO NOTHING.
 * issued_coupons.coupon_event_id 는 coupon_events FK 라서 이벤트 행을 JdbcTemplate 으로 먼저 넣는다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, IssuedCouponRepositoryImpl.class})
class IssuedCouponRepositoryImplTest {

    /** PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘 시각 */
    private static final Instant ISSUED_AT = Instant.parse("2026-11-20T10:00:00.123456Z");
    private static final Instant START_AT = Instant.parse("2026-11-20T09:00:00Z");

    @Autowired IssuedCouponRepositoryImpl repository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    private long eventA;
    private long eventB;
    private long eventC;

    @BeforeEach
    void insertEvents() {
        eventA = insertEvent("A");
        eventB = insertEvent("B");
        eventC = insertEvent("C");
    }

    private long insertEvent(String name) {
        return jdbcTemplate.queryForObject(
                "insert into coupon_events (name, total_quantity, issue_start_at, created_at)"
                        + " values (?, 100, ?, ?) returning id",
                Long.class, name, Timestamp.from(START_AT), Timestamp.from(START_AT.minusSeconds(3600)));
    }

    private long countRows(long couponEventId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from issued_coupons where coupon_event_id = ?", Long.class, couponEventId);
    }

    @Test
    @DisplayName("[T6-11] 같은 (event, user) 를 2번 saveIfAbsent 하면 true, false 이고 예외 없이 행 1개, 첫 값이 유지된다")
    void duplicateIgnored() {
        // Given
        boolean first = repository.saveIfAbsent(IssuedCoupon.issue(eventA, 1L, ISSUED_AT));

        // When: 재전달 — issuedAt 이 달라도 (event, user) 가 같으면 충돌
        boolean[] second = new boolean[1];
        assertThatCode(() -> second[0] = repository.saveIfAbsent(
                IssuedCoupon.issue(eventA, 1L, ISSUED_AT.plusSeconds(5))))
                .doesNotThrowAnyException();

        // Then
        assertThat(first).isTrue();
        assertThat(second[0]).isFalse();
        assertThat(countRows(eventA)).isEqualTo(1L);
        entityManager.clear();
        assertThat(repository.findByUserId(1L))
                .singleElement()
                .satisfies(c -> assertThat(c.getIssuedAt()).isEqualTo(ISSUED_AT));
    }

    @Test
    @DisplayName("[T6-11] 충돌 뒤에도 같은 트랜잭션에서 다음 저장이 된다 (예외로 rollback-only 가 되지 않음)")
    void conflictDoesNotPoisonTransaction() {
        // Given
        repository.saveIfAbsent(IssuedCoupon.issue(eventA, 1L, ISSUED_AT));
        repository.saveIfAbsent(IssuedCoupon.issue(eventA, 1L, ISSUED_AT));

        // When
        boolean next = repository.saveIfAbsent(IssuedCoupon.issue(eventA, 2L, ISSUED_AT));

        // Then
        assertThat(next).isTrue();
        assertThat(countRows(eventA)).isEqualTo(2L);
    }

    @Test
    @DisplayName("[T6-11] 충돌 대상은 (event, user) — 같은 이벤트 다른 사용자, 같은 사용자 다른 이벤트는 각각 저장된다")
    void conflictScopeIsEventAndUser() {
        // Given
        repository.saveIfAbsent(IssuedCoupon.issue(eventA, 1L, ISSUED_AT));

        // When
        boolean otherUser = repository.saveIfAbsent(IssuedCoupon.issue(eventA, 2L, ISSUED_AT));
        boolean otherEvent = repository.saveIfAbsent(IssuedCoupon.issue(eventB, 1L, ISSUED_AT));

        // Then
        assertThat(otherUser).isTrue();
        assertThat(otherEvent).isTrue();
        assertThat(countRows(eventA)).isEqualTo(2L);
        assertThat(countRows(eventB)).isEqualTo(1L);
    }

    @Test
    @DisplayName("[T6-11] findByUserId 는 해당 사용자 쿠폰만 issuedAt 최신순으로, id·필드(마이크로초 포함)를 복원해 돌려준다")
    void findByUserIdLatestFirst() {
        // Given: 저장 순서를 issuedAt 순서와 다르게 둔다. 다른 사용자 쿠폰도 섞는다
        repository.saveIfAbsent(IssuedCoupon.issue(eventA, 5L, ISSUED_AT.plusSeconds(60)));
        repository.saveIfAbsent(IssuedCoupon.issue(eventB, 5L, ISSUED_AT.plusSeconds(180)));
        repository.saveIfAbsent(IssuedCoupon.issue(eventC, 5L, ISSUED_AT));
        repository.saveIfAbsent(IssuedCoupon.issue(eventA, 6L, ISSUED_AT.plusSeconds(300)));
        entityManager.clear();

        // When
        List<IssuedCoupon> mine = repository.findByUserId(5L);

        // Then
        assertThat(mine).extracting(IssuedCoupon::getCouponEventId).containsExactly(eventB, eventA, eventC);
        assertThat(mine).allSatisfy(c -> {
            assertThat(c.getId()).isNotNull();
            assertThat(c.getUserId()).isEqualTo(5L);
        });
        assertThat(mine.getFirst().getIssuedAt()).isEqualTo(ISSUED_AT.plusSeconds(180));
        assertThat(repository.findByUserId(999L)).isEmpty();
    }

    @Test
    @DisplayName("[T6-11] V2 스키마: UK uk_issued_coupons_event_user 와 내 쿠폰 조회 인덱스 idx_issued_coupons_user 가 있다")
    void schemaHasUniqueAndUserIndex() {
        // When
        List<String> indexes = jdbcTemplate.queryForList(
                "select indexname from pg_indexes where tablename = 'issued_coupons'", String.class);

        // Then
        assertThat(indexes).contains("uk_issued_coupons_event_user", "idx_issued_coupons_user");
    }
}
