package com.queuedrop.product.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.product.application.dto.DropPageResult;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.DropSearchQuery;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.service.fake.FakeDropRepository;
import com.queuedrop.product.domain.model.Drop;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DropQueryServiceTest {

    /** 조회 기준 시각 */
    private static final Instant NOW = Instant.parse("2026-11-16T11:00:00Z");
    private static final Instant CREATED_AT = Instant.parse("2026-11-15T00:00:00Z");

    private FakeDropRepository dropRepository;
    private DropQueryService queryService;

    private Drop scheduled;   // 12:00 ~ 14:00
    private Drop openLate;    // 10:00 ~ 12:00
    private Drop openEarly;   // 09:00 ~ 13:00
    private Drop closed;      // 07:00 ~ 09:00
    private Drop drawn;       // 06:00 ~ 08:00, drawn

    @BeforeEach
    void setUp() {
        dropRepository = new FakeDropRepository();
        queryService = new DropQueryService(dropRepository, Clock.fixed(NOW, ZoneOffset.UTC));

        // 저장 순서와 openAt 순서를 일부러 다르게 둔다
        scheduled = givenDrop("예정", "2026-11-16T12:00:00Z", "2026-11-16T14:00:00Z", false);
        openLate = givenDrop("진행-늦게", "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z", false);
        openEarly = givenDrop("진행-일찍", "2026-11-16T09:00:00Z", "2026-11-16T13:00:00Z", false);
        closed = givenDrop("마감", "2026-11-16T07:00:00Z", "2026-11-16T09:00:00Z", false);
        drawn = givenDrop("추첨완료", "2026-11-16T06:00:00Z", "2026-11-16T08:00:00Z", true);
    }

    /** create 의 1분 여유 검증을 피해 과거 시각 드로우도 둘 수 있도록 restore 로 저장한다. */
    private Drop givenDrop(String title, String openAt, String closeAt, boolean isDrawn) {
        return dropRepository.save(Drop.restore(null, 1L, title, 100, 10,
                Instant.parse(openAt), Instant.parse(closeAt), isDrawn, CREATED_AT));
    }

    @ParameterizedTest(name = "[T2-16] status=\"{0}\"")
    @ValueSource(strings = {"FOO", "open", "Open", " OPEN"})
    @DisplayName("[T2-16] 알 수 없는 상태 값(대소문자 구분)이면 PRODUCT_INVALID_STATUS_FILTER")
    void rejectsUnknownStatus(String status) {
        // Given
        DropSearchQuery query = new DropSearchQuery(status, 0, 20);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> queryService.search(query))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(ProductErrorCode.INVALID_STATUS_FILTER));
    }

    @Test
    @DisplayName("[T2-16] 없는 dropId 를 get 하면 PRODUCT_DROP_NOT_FOUND")
    void getUnknownDrop() {
        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> queryService.get(999L))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(ProductErrorCode.DROP_NOT_FOUND));
    }

    @Test
    @DisplayName("[T2-16] get 은 모든 필드와 now 기준 status 문자열을 돌려준다")
    void getDrop() {
        // When
        DropResult result = queryService.get(openLate.getId());

        // Then
        assertThat(result.id()).isEqualTo(openLate.getId());
        assertThat(result.productId()).isEqualTo(1L);
        assertThat(result.title()).isEqualTo("진행-늦게");
        assertThat(result.entryCapacity()).isEqualTo(100);
        assertThat(result.winnerCount()).isEqualTo(10);
        assertThat(result.openAt()).isEqualTo(Instant.parse("2026-11-16T10:00:00Z"));
        assertThat(result.closeAt()).isEqualTo(Instant.parse("2026-11-16T12:00:00Z"));
        assertThat(result.status()).isEqualTo("OPEN");
        assertThat(result.createdAt()).isEqualTo(CREATED_AT);
    }

    @Test
    @DisplayName("[T2-16] drawn 드로우를 get 하면 status 는 DRAWN")
    void getDrawnDrop() {
        // When / Then
        assertThat(queryService.get(drawn.getId()).status()).isEqualTo("DRAWN");
    }

    @Test
    @DisplayName("[T2-16] status=OPEN 이면 진행 중인 드로우만 openAt 순으로, status 문자열 OPEN 으로 돌려준다")
    void searchOpen() {
        // Given
        DropSearchQuery query = new DropSearchQuery("OPEN", 0, 20);

        // When
        DropPageResult result = queryService.search(query);

        // Then
        assertThat(result.content()).extracting(DropResult::id)
                .containsExactly(openEarly.getId(), openLate.getId());
        assertThat(result.content()).extracting(DropResult::status).containsOnly("OPEN");
        assertThat(result.totalElements()).isEqualTo(2);
        assertThat(result.page()).isZero();
        assertThat(result.size()).isEqualTo(20);
    }

    @ParameterizedTest(name = "[T2-16] status={0}")
    @CsvSource({"SCHEDULED", "CLOSED", "DRAWN"})
    @DisplayName("[T2-16] 각 상태 필터는 해당 상태 드로우 1건만 돌려준다")
    void searchEachStatus(String status) {
        // When
        DropPageResult result = queryService.search(new DropSearchQuery(status, 0, 20));

        // Then
        assertThat(result.content()).hasSize(1);
        assertThat(result.content().getFirst().status()).isEqualTo(status);
        assertThat(result.totalElements()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[T2-16] status=\"{0}\"")
    @NullSource
    @ValueSource(strings = {"", "   "})
    @DisplayName("[T2-16] status 가 null 이나 공백이면 전체를 openAt 순으로 돌려준다")
    void searchAll(String status) {
        // When
        DropPageResult result = queryService.search(new DropSearchQuery(status, 0, 20));

        // Then
        assertThat(result.content()).extracting(DropResult::id).containsExactly(
                drawn.getId(), closed.getId(), openEarly.getId(), openLate.getId(), scheduled.getId());
        assertThat(result.content()).extracting(DropResult::status).containsExactly(
                "DRAWN", "CLOSED", "OPEN", "OPEN", "SCHEDULED");
        assertThat(result.totalElements()).isEqualTo(5);
    }

    @Test
    @DisplayName("[T2-16] 페이징: size=2 이면 page 0·1·2 가 2·2·1건이고 totalElements 는 5")
    void searchPaging() {
        // When
        DropPageResult first = queryService.search(new DropSearchQuery(null, 0, 2));
        DropPageResult second = queryService.search(new DropSearchQuery(null, 1, 2));
        DropPageResult last = queryService.search(new DropSearchQuery(null, 2, 2));

        // Then
        assertThat(first.content()).extracting(DropResult::id).containsExactly(drawn.getId(), closed.getId());
        assertThat(second.content()).extracting(DropResult::id).containsExactly(openEarly.getId(), openLate.getId());
        assertThat(last.content()).extracting(DropResult::id).containsExactly(scheduled.getId());
        assertThat(first.totalElements()).isEqualTo(5);
        assertThat(last.totalElements()).isEqualTo(5);
        assertThat(second.page()).isEqualTo(1);
        assertThat(second.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("[T2-16] 범위를 넘는 page 는 빈 content 와 전체 건수를 돌려준다")
    void searchBeyondLastPage() {
        // When
        DropPageResult result = queryService.search(new DropSearchQuery(null, 10, 2));

        // Then
        assertThat(result.content()).isEmpty();
        assertThat(result.totalElements()).isEqualTo(5);
        assertThat(result.page()).isEqualTo(10);
    }

    @Test
    @DisplayName("[T2-16] openAt 이 같으면 id 순으로 정렬된다")
    void tieBreakById() {
        // Given: openLate 와 같은 openAt
        Drop sameOpenAt = givenDrop("진행-동시", "2026-11-16T10:00:00Z", "2026-11-16T15:00:00Z", false);

        // When
        DropPageResult result = queryService.search(new DropSearchQuery("OPEN", 0, 20));

        // Then
        assertThat(result.content()).extracting(DropResult::id)
                .containsExactly(openEarly.getId(), openLate.getId(), sameOpenAt.getId());
    }
}
