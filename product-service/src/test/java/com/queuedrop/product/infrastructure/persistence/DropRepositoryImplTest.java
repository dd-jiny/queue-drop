package com.queuedrop.product.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;
import com.queuedrop.product.domain.model.Product;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, ProductRepositoryImpl.class, ProductMapper.class,
        DropRepositoryImpl.class, DropMapper.class})
class DropRepositoryImplTest {

    /** 조회 기준 시각. PostgreSQL timestamptz 정밀도(마이크로초)에 맞춘다. */
    private static final Instant NOW = Instant.parse("2026-11-16T11:00:00.123456Z");
    private static final Duration MICRO = Duration.ofNanos(1_000);
    private static final Instant CREATED_AT = Instant.parse("2026-11-15T09:30:00.654321Z");

    @Autowired ProductRepositoryImpl productRepository;
    @Autowired DropRepositoryImpl dropRepository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired EntityManagerFactory entityManagerFactory;

    private Long givenProductId() {
        return productRepository.save(Product.create("한정판 스니커즈", null, null, CREATED_AT)).getId();
    }

    /** create 의 1분 여유 검증을 피해 과거 시각 드로우도 만들 수 있도록 restore(id=null) 로 만든다. */
    private Drop newDrop(Long productId, String title, Instant openAt, Instant closeAt, boolean drawn) {
        return Drop.restore(null, productId, title, 100, 10, openAt, closeAt, drawn, CREATED_AT);
    }

    private Long insertProductByJdbc() {
        return jdbcTemplate.queryForObject(
                "insert into products (name, created_at) values ('상품', now()) returning id", Long.class);
    }

    // ---------------------------------------------------------------- T2-18

    @Test
    @DisplayName("[T2-18] 드로우 save 후 findById 로 다시 읽으면 id 가 부여되고 모든 필드(마이크로초 Instant 포함)가 일치한다")
    void saveAndFindRoundTrip() {
        // Given
        Long productId = givenProductId();
        Instant openAt = Instant.parse("2026-11-16T10:00:00.000001Z");
        Instant closeAt = Instant.parse("2026-11-16T12:00:00.999999Z");
        Drop saved = dropRepository.save(
                Drop.restore(null, productId, "한정판 스니커즈 드로우", 100, 7, openAt, closeAt, false, CREATED_AT));
        entityManager.clear();   // 1차 캐시가 아니라 DB 에서 다시 읽게 한다

        // When
        Drop found = dropRepository.findById(saved.getId()).orElseThrow();

        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(found.getId()).isEqualTo(saved.getId());
        assertThat(found.getProductId()).isEqualTo(productId);
        assertThat(found.getTitle()).isEqualTo("한정판 스니커즈 드로우");
        assertThat(found.getEntryCapacity()).isEqualTo(100);
        assertThat(found.getWinnerCount()).isEqualTo(7);
        assertThat(found.getOpenAt()).isEqualTo(openAt);
        assertThat(found.getCloseAt()).isEqualTo(closeAt);
        assertThat(found.isDrawn()).isFalse();
        assertThat(found.getCreatedAt()).isEqualTo(CREATED_AT);
    }

    @Test
    @DisplayName("[T2-18] drawn=true 로 저장한 드로우는 drawn=true 로 복원된다")
    void drawnRoundTrip() {
        // Given
        Long productId = givenProductId();
        Drop saved = dropRepository.save(newDrop(productId, "추첨완료", NOW.minusSeconds(7200), NOW.minusSeconds(3600), true));
        entityManager.clear();

        // When
        Drop found = dropRepository.findById(saved.getId()).orElseThrow();

        // Then
        assertThat(found.isDrawn()).isTrue();
    }

    @Test
    @DisplayName("[T2-18] 없는 id 를 findById 하면 빈 값")
    void findMissing() {
        // When / Then
        assertThat(dropRepository.findById(999_999L)).isEmpty();
    }

    @Test
    @DisplayName("[T2-18] 조회한 드로우에 markDrawn() 후 save 하면 같은 행이 갱신되어 drawn=true 로 다시 읽힌다 (5단계 대비)")
    void markDrawnThenSaveUpdates() {
        // Given
        Long productId = givenProductId();
        Drop saved = dropRepository.save(newDrop(productId, "드로우", NOW.minusSeconds(7200), NOW.minusSeconds(3600), false));
        entityManager.clear();
        Drop loaded = dropRepository.findById(saved.getId()).orElseThrow();

        // When
        loaded.markDrawn();
        Drop updated = dropRepository.save(loaded);
        entityManager.flush();
        entityManager.clear();

        // Then
        Drop found = dropRepository.findById(saved.getId()).orElseThrow();
        assertThat(updated.getId()).isEqualTo(saved.getId());
        assertThat(found.isDrawn()).isTrue();
        assertThat(found.status(NOW)).isEqualTo(DropStatus.DRAWN);
        assertThat(found.getTitle()).isEqualTo("드로우");
        assertThat(found.getCreatedAt()).isEqualTo(CREATED_AT);
        assertThat(jdbcTemplate.queryForObject("select count(*) from drops", Long.class)).isEqualTo(1L);
    }

    // ---------------------------------------------------------------- T2-19

    /** 상태별 데이터 + 경계 시각. 저장 순서는 openAt 순서와 일부러 다르게 둔다. */
    private List<Drop> givenDropsAroundNow() {
        Long productId = givenProductId();
        List<Drop> drops = new ArrayList<>();
        // SCHEDULED
        drops.add(dropRepository.save(newDrop(productId, "예정", NOW.plusSeconds(3600), NOW.plusSeconds(7200), false)));
        drops.add(dropRepository.save(newDrop(productId, "예정-경계(openAt=now+1µs)", NOW.plus(MICRO), NOW.plusSeconds(7200), false)));
        // OPEN
        drops.add(dropRepository.save(newDrop(productId, "진행-경계(openAt=now)", NOW, NOW.plusSeconds(3600), false)));
        drops.add(dropRepository.save(newDrop(productId, "진행-경계(closeAt=now+1µs)", NOW.minusSeconds(3600), NOW.plus(MICRO), false)));
        drops.add(dropRepository.save(newDrop(productId, "진행-동시오픈(openAt=now, id 후순위)", NOW, NOW.plusSeconds(1800), false)));
        // CLOSED
        drops.add(dropRepository.save(newDrop(productId, "마감-경계(closeAt=now)", NOW.minusSeconds(7200), NOW, false)));
        drops.add(dropRepository.save(newDrop(productId, "마감", NOW.minusSeconds(10800), NOW.minusSeconds(9000), false)));
        // DRAWN (시각상 CLOSED, 시각상 OPEN 인 것 모두 DRAWN 이 우선)
        drops.add(dropRepository.save(newDrop(productId, "추첨완료", NOW.minusSeconds(14400), NOW.minusSeconds(12600), true)));
        drops.add(dropRepository.save(newDrop(productId, "추첨완료-시각상진행", NOW.minusSeconds(1800), NOW.plusSeconds(1800), true)));
        entityManager.flush();
        entityManager.clear();
        return drops;
    }

    private static List<Long> expectedIds(List<Drop> all, DropStatus status) {
        return all.stream()
                .filter(d -> status == null || d.status(NOW) == status)
                .sorted(Comparator.comparing(Drop::getOpenAt).thenComparing(Drop::getId))
                .map(Drop::getId)
                .toList();
    }

    @ParameterizedTest(name = "[T2-19] status={0}")
    @EnumSource(DropStatus.class)
    @DisplayName("[T2-19] 상태별 search 결과가 Drop.status(now) 분류와 같고 openAt→id 순으로 정렬된다")
    void searchMatchesDomainStatus(DropStatus status) {
        // Given
        List<Drop> all = givenDropsAroundNow();

        // When
        DropPage page = dropRepository.search(status, NOW, 0, 20);

        // Then
        List<Long> expected = expectedIds(all, status);
        assertThat(page.content()).extracting(Drop::getId).containsExactlyElementsOf(expected);
        assertThat(page.content()).allSatisfy(d -> assertThat(d.status(NOW)).isEqualTo(status));
        assertThat(page.totalElements()).isEqualTo(expected.size());
        assertThat(page.page()).isZero();
        assertThat(page.size()).isEqualTo(20);
    }

    @ParameterizedTest(name = "[T2-19] status={0} → {1}건")
    @CsvSource({"SCHEDULED, 2", "OPEN, 3", "CLOSED, 2", "DRAWN, 2"})
    @DisplayName("[T2-19] 경계 시각 분류 건수: openAt=now 는 OPEN, closeAt=now 는 CLOSED, drawn 은 시각보다 우선")
    void boundaryCounts(DropStatus status, long count) {
        // Given
        givenDropsAroundNow();

        // When
        DropPage page = dropRepository.search(status, NOW, 0, 20);

        // Then
        assertThat(page.totalElements()).isEqualTo(count);
    }

    @Test
    @DisplayName("[T2-19] 경계 케이스 개별 확인: openAt=now 드로우는 OPEN 에만, closeAt=now 드로우는 CLOSED 에만 나온다")
    void boundaryDropsClassified() {
        // Given
        givenDropsAroundNow();

        // When
        List<String> open = dropRepository.search(DropStatus.OPEN, NOW, 0, 20).content().stream().map(Drop::getTitle).toList();
        List<String> closed = dropRepository.search(DropStatus.CLOSED, NOW, 0, 20).content().stream().map(Drop::getTitle).toList();
        List<String> scheduled = dropRepository.search(DropStatus.SCHEDULED, NOW, 0, 20).content().stream().map(Drop::getTitle).toList();

        // Then
        assertThat(open).contains("진행-경계(openAt=now)", "진행-경계(closeAt=now+1µs)").doesNotContain("마감-경계(closeAt=now)", "추첨완료-시각상진행");
        assertThat(closed).contains("마감-경계(closeAt=now)").doesNotContain("진행-경계(openAt=now)", "추첨완료");
        assertThat(scheduled).contains("예정-경계(openAt=now+1µs)").doesNotContain("진행-경계(openAt=now)");
    }

    @Test
    @DisplayName("[T2-19] status=null 이면 전체를 openAt→id 순으로 돌려준다")
    void searchAll() {
        // Given
        List<Drop> all = givenDropsAroundNow();

        // When
        DropPage page = dropRepository.search(null, NOW, 0, 20);

        // Then
        assertThat(page.content()).extracting(Drop::getId).containsExactlyElementsOf(expectedIds(all, null));
        assertThat(page.totalElements()).isEqualTo(9);
    }

    @Test
    @DisplayName("[T2-19] 페이징: page=1, size=3 이면 정렬 순서의 4~6번째와 전체 건수 9 를 돌려준다")
    void searchPaging() {
        // Given
        List<Drop> all = givenDropsAroundNow();
        List<Long> sorted = expectedIds(all, null);

        // When
        DropPage page = dropRepository.search(null, NOW, 1, 3);

        // Then
        assertThat(page.content()).extracting(Drop::getId).containsExactlyElementsOf(sorted.subList(3, 6));
        assertThat(page.totalElements()).isEqualTo(9);
        assertThat(page.page()).isEqualTo(1);
        assertThat(page.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("[T2-19] 상태 필터 + 페이징: OPEN 3건을 size=2 로 나누면 page 1 은 1건, totalElements 는 3")
    void searchFilteredPaging() {
        // Given
        List<Drop> all = givenDropsAroundNow();
        List<Long> open = expectedIds(all, DropStatus.OPEN);

        // When
        DropPage page = dropRepository.search(DropStatus.OPEN, NOW, 1, 2);

        // Then
        assertThat(page.content()).extracting(Drop::getId).containsExactly(open.get(2));
        assertThat(page.totalElements()).isEqualTo(3);
    }

    // ---------------------------------------------------------------- T2-20

    @Test
    @DisplayName("[T2-20] 슬라이스 기동 시 Flyway V1 이 적용되고 ddl-auto 는 validate 이다")
    void flywayAppliedAndSchemaValidated() {
        // When
        String appliedVersion = jdbcTemplate.queryForObject(
                "select version from flyway_schema_history where success order by installed_rank desc limit 1",
                String.class);

        // Then
        assertThat(appliedVersion).isEqualTo("1");
        assertThat(entityManagerFactory.getProperties()).containsEntry("hibernate.hbm2ddl.auto", "validate");
    }

    /** PostgreSQL 은 실패한 문장 이후 트랜잭션이 중단되므로 위반 INSERT 는 테스트당 하나만 실행한다. */
    @ParameterizedTest(name = "[T2-20] {0}")
    @CsvSource(delimiter = '|', value = {
            "ck_drops_winner_count | 10 | 0  | 2026-11-16T10:00:00Z | 2026-11-16T12:00:00Z",
            "ck_drops_capacity     | 10 | 11 | 2026-11-16T10:00:00Z | 2026-11-16T12:00:00Z",
            "ck_drops_period       | 10 | 5  | 2026-11-16T12:00:00Z | 2026-11-16T12:00:00Z",
            "ck_drops_period       | 10 | 5  | 2026-11-16T13:00:00Z | 2026-11-16T12:00:00Z"
    })
    @DisplayName("[T2-20] CHECK 제약 위반 INSERT 는 실패한다")
    void checkConstraintsRejectInvalidRows(String constraint, int entryCapacity, int winnerCount,
                                           String openAt, String closeAt) {
        // Given
        Long productId = insertProductByJdbc();

        // When / Then
        assertThatThrownBy(() -> jdbcTemplate.update(
                "insert into drops (product_id, title, entry_capacity, winner_count, open_at, close_at, created_at) "
                        + "values (?, '드로우', ?, ?, ?::timestamptz, ?::timestamptz, now())",
                productId, entryCapacity, winnerCount, openAt, closeAt))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining(constraint);
    }

    @Test
    @DisplayName("[T2-20] 정상 행 INSERT 는 성공하고 drawn 기본값은 false 이다 (CHECK 위반 테스트의 대조군)")
    void validRowInserted() {
        // Given
        Long productId = insertProductByJdbc();

        // When
        jdbcTemplate.update(
                "insert into drops (product_id, title, entry_capacity, winner_count, open_at, close_at, created_at) "
                        + "values (?, '드로우', 10, 10, '2026-11-16T10:00:00Z', '2026-11-16T12:00:00Z', now())",
                productId);

        // Then
        assertThat(jdbcTemplate.queryForObject("select drawn from drops", Boolean.class)).isFalse();
    }

    @Test
    @DisplayName("[T2-20] 존재하지 않는 product_id 로 INSERT 하면 FK 위반으로 실패한다")
    void foreignKeyRejectsUnknownProduct() {
        // When / Then
        assertThatThrownBy(() -> jdbcTemplate.update(
                "insert into drops (product_id, title, entry_capacity, winner_count, open_at, close_at, created_at) "
                        + "values (999999, '드로우', 10, 5, '2026-11-16T10:00:00Z', '2026-11-16T12:00:00Z', now())"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
