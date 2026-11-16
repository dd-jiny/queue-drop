package com.queuedrop.product.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.product.domain.exception.DomainException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DropTest {

    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");
    private static final Instant CREATED_AT = Instant.parse("2026-11-15T09:00:00Z");

    /** 등록(create) 기준 시각: 오픈 1시간 전 */
    private static final Instant NOW = Instant.parse("2026-11-16T09:00:00Z");

    private static final Long PRODUCT_ID = 1L;
    private static final String TITLE = "한정판 스니커즈 드로우";

    /** 상태 경계 테스트용: create 의 1분 여유 검증을 거치지 않도록 restore 로 만든다 */
    private static Drop restoredDrop(boolean drawn) {
        return Drop.restore(10L, PRODUCT_ID, TITLE, 100, 10, OPEN_AT, CLOSE_AT, drawn, CREATED_AT);
    }

    @Nested
    @DisplayName("status(now) — [openAt, closeAt) 반열린 구간")
    class Status {

        @Test
        @DisplayName("[T2-01] openAt 직전(09:59:59.999)이면 SCHEDULED")
        void scheduledJustBeforeOpen() {
            // Given
            Drop drop = restoredDrop(false);

            // When
            DropStatus status = drop.status(Instant.parse("2026-11-16T09:59:59.999Z"));

            // Then
            assertThat(status).isEqualTo(DropStatus.SCHEDULED);
        }

        @Test
        @DisplayName("[T2-02] openAt 정각(10:00:00)이면 OPEN")
        void openAtExactOpen() {
            // Given
            Drop drop = restoredDrop(false);

            // When
            DropStatus status = drop.status(OPEN_AT);

            // Then
            assertThat(status).isEqualTo(DropStatus.OPEN);
        }

        @Test
        @DisplayName("[T2-03] closeAt 직전(11:59:59.999)이면 OPEN")
        void openJustBeforeClose() {
            // Given
            Drop drop = restoredDrop(false);

            // When
            DropStatus status = drop.status(Instant.parse("2026-11-16T11:59:59.999Z"));

            // Then
            assertThat(status).isEqualTo(DropStatus.OPEN);
        }

        @Test
        @DisplayName("[T2-04] closeAt 정각(12:00:00)이면 CLOSED")
        void closedAtExactClose() {
            // Given
            Drop drop = restoredDrop(false);

            // When
            DropStatus status = drop.status(CLOSE_AT);

            // Then
            assertThat(status).isEqualTo(DropStatus.CLOSED);
        }

        @Test
        @DisplayName("[T2-05] markDrawn() 후에는 마감 후 임의 시각에 DRAWN, 두 번 호출해도 DRAWN (멱등)")
        void drawnAfterMarkDrawn() {
            // Given
            Drop drop = restoredDrop(false);
            Instant afterClose = Instant.parse("2026-11-17T00:00:00Z");

            // When
            drop.markDrawn();

            // Then
            assertThat(drop.isDrawn()).isTrue();
            assertThat(drop.status(afterClose)).isEqualTo(DropStatus.DRAWN);

            // When: 두 번째 호출
            drop.markDrawn();

            // Then
            assertThat(drop.isDrawn()).isTrue();
            assertThat(drop.status(afterClose)).isEqualTo(DropStatus.DRAWN);
        }

        @Test
        @DisplayName("[T2-05] drawn=true 로 복원된 드로우는 시각과 무관하게 DRAWN (저장 값 우선)")
        void drawnTakesPrecedenceOverTime() {
            // Given
            Drop drop = restoredDrop(true);

            // When / Then
            assertThat(drop.status(Instant.parse("2026-11-16T09:59:59.999Z"))).isEqualTo(DropStatus.DRAWN);
            assertThat(drop.status(OPEN_AT)).isEqualTo(DropStatus.DRAWN);
            assertThat(drop.status(CLOSE_AT)).isEqualTo(DropStatus.DRAWN);
        }
    }

    @Nested
    @DisplayName("create — 등록 검증")
    class Create {

        @Test
        @DisplayName("[T2-06] 응모 정원 M=10, 당첨 수량 K=11 이면 DomainException")
        void rejectsCapacityLessThanWinnerCount() {
            // Given
            int entryCapacity = 10;
            int winnerCount = 11;

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, entryCapacity, winnerCount, OPEN_AT, CLOSE_AT, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @ParameterizedTest(name = "[T2-07] K={0}")
        @ValueSource(ints = {0, -1})
        @DisplayName("[T2-07] 당첨 수량 K 가 1 미만이면 DomainException")
        void rejectsWinnerCountBelowOne(int winnerCount) {
            // Given: winnerCount

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 10, winnerCount, OPEN_AT, CLOSE_AT, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-07] M=K 이면 생성된다 (경계값)")
        void acceptsCapacityEqualToWinnerCount() {
            // Given
            int capacity = 10;

            // When
            Drop drop = Drop.create(PRODUCT_ID, TITLE, capacity, capacity, OPEN_AT, CLOSE_AT, NOW);

            // Then
            assertThat(drop.getEntryCapacity()).isEqualTo(10);
            assertThat(drop.getWinnerCount()).isEqualTo(10);
        }

        @Test
        @DisplayName("[T2-07] M=1, K=1 이면 생성된다 (최소값)")
        void acceptsMinimumCounts() {
            // Given / When
            Drop drop = Drop.create(PRODUCT_ID, TITLE, 1, 1, OPEN_AT, CLOSE_AT, NOW);

            // Then
            assertThat(drop.getEntryCapacity()).isEqualTo(1);
            assertThat(drop.getWinnerCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("[T2-08] openAt == closeAt 이면 DomainException")
        void rejectsOpenEqualsClose() {
            // Given
            Instant same = OPEN_AT;

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, same, same, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-08] openAt > closeAt 이면 DomainException")
        void rejectsOpenAfterClose() {
            // Given
            Instant openAt = CLOSE_AT;
            Instant closeAt = OPEN_AT;

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, openAt, closeAt, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-09] now=10:00, openAt=10:00:59 (여유 1분 미만)이면 DomainException")
        void rejectsOpenWithinLeadTime() {
            // Given
            Instant now = Instant.parse("2026-11-16T10:00:00Z");
            Instant openAt = Instant.parse("2026-11-16T10:00:59Z");
            Instant closeAt = Instant.parse("2026-11-16T12:00:00Z");

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, openAt, closeAt, now))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-09] now=10:00, openAt=10:01:00 (여유 정확히 1분)이면 생성된다")
        void acceptsOpenExactlyAtLeadTime() {
            // Given
            Instant now = Instant.parse("2026-11-16T10:00:00Z");
            Instant openAt = Instant.parse("2026-11-16T10:01:00Z");
            Instant closeAt = Instant.parse("2026-11-16T12:00:00Z");

            // When
            Drop drop = Drop.create(PRODUCT_ID, TITLE, 100, 10, openAt, closeAt, now);

            // Then
            assertThat(drop.getOpenAt()).isEqualTo(openAt);
        }

        @Test
        @DisplayName("[T2-09] 최소 오픈 여유는 1분이다")
        void minOpenLeadTimeIsOneMinute() {
            // Then
            assertThat(Drop.MIN_OPEN_LEAD_TIME).isEqualTo(Duration.ofMinutes(1));
        }

        @Test
        @DisplayName("[T2-09] openAt 이 과거이면 DomainException")
        void rejectsPastOpenAt() {
            // Given
            Instant now = Instant.parse("2026-11-16T10:30:00Z");

            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, OPEN_AT, CLOSE_AT, now))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-06~09] productId 가 null 이면 DomainException")
        void rejectsNullProductId() {
            // When / Then
            assertThatThrownBy(() -> Drop.create(null, TITLE, 100, 10, OPEN_AT, CLOSE_AT, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @ParameterizedTest(name = "[T2-06~09] 제목 \"{0}\"")
        @NullSource
        @ValueSource(strings = {"", "   "})
        @DisplayName("[T2-06~09] 제목이 null, 빈 문자열, 공백만이면 DomainException")
        void rejectsBlankTitle(String title) {
            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, title, 100, 10, OPEN_AT, CLOSE_AT, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-08] openAt 이 null 이면 DomainException")
        void rejectsNullOpenAt() {
            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, null, CLOSE_AT, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-08] closeAt 이 null 이면 DomainException")
        void rejectsNullCloseAt() {
            // When / Then
            assertThatThrownBy(() -> Drop.create(PRODUCT_ID, TITLE, 100, 10, OPEN_AT, null, NOW))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T2-06~09] 정상 입력이면 생성되고 id 는 null, drawn 은 false, createdAt 은 now 이다")
        void createsDropWithFields() {
            // When
            Drop drop = Drop.create(PRODUCT_ID, TITLE, 100, 10, OPEN_AT, CLOSE_AT, NOW);

            // Then
            assertThat(drop.getId()).isNull();
            assertThat(drop.getProductId()).isEqualTo(PRODUCT_ID);
            assertThat(drop.getTitle()).isEqualTo(TITLE);
            assertThat(drop.getEntryCapacity()).isEqualTo(100);
            assertThat(drop.getWinnerCount()).isEqualTo(10);
            assertThat(drop.getOpenAt()).isEqualTo(OPEN_AT);
            assertThat(drop.getCloseAt()).isEqualTo(CLOSE_AT);
            assertThat(drop.isDrawn()).isFalse();
            assertThat(drop.getCreatedAt()).isEqualTo(NOW);
            assertThat(drop.status(NOW)).isEqualTo(DropStatus.SCHEDULED);
        }
    }

    @Test
    @DisplayName("[T2-01~05] restore 는 저장된 값을 그대로 복원한다")
    void restoresDrop() {
        // When
        Drop drop = restoredDrop(true);

        // Then
        assertThat(drop.getId()).isEqualTo(10L);
        assertThat(drop.getProductId()).isEqualTo(PRODUCT_ID);
        assertThat(drop.getTitle()).isEqualTo(TITLE);
        assertThat(drop.getEntryCapacity()).isEqualTo(100);
        assertThat(drop.getWinnerCount()).isEqualTo(10);
        assertThat(drop.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(drop.getCloseAt()).isEqualTo(CLOSE_AT);
        assertThat(drop.isDrawn()).isTrue();
        assertThat(drop.getCreatedAt()).isEqualTo(CREATED_AT);
    }
}
