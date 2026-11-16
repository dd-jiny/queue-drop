package com.queuedrop.queue.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.queue.domain.exception.DomainException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueueMetaTest {

    private static final Long DROP_ID = 10L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");

    private static final Instant JUST_BEFORE_OPEN = Instant.parse("2026-11-16T09:59:59.999Z");
    private static final Instant JUST_BEFORE_CLOSE = Instant.parse("2026-11-16T11:59:59.999Z");

    private static QueueMeta meta() {
        return new QueueMeta(DROP_ID, OPEN_AT, CLOSE_AT);
    }

    @Nested
    @DisplayName("isOpen / isBeforeOpen / isClosed — [openAt, closeAt) 반열린 구간")
    class OpenWindow {

        @Test
        @DisplayName("[T3-01] openAt 직전(09:59:59.999)이면 열리기 전 — isOpen=false, isBeforeOpen=true, isClosed=false")
        void justBeforeOpen() {
            // Given
            QueueMeta meta = meta();

            // When
            boolean open = meta.isOpen(JUST_BEFORE_OPEN);
            boolean beforeOpen = meta.isBeforeOpen(JUST_BEFORE_OPEN);
            boolean closed = meta.isClosed(JUST_BEFORE_OPEN);

            // Then
            assertThat(open).isFalse();
            assertThat(beforeOpen).isTrue();
            assertThat(closed).isFalse();
        }

        @Test
        @DisplayName("[T3-01] openAt 정각(10:00)이면 열림 — isOpen=true, isBeforeOpen=false, isClosed=false")
        void exactlyOpenAt() {
            // Given
            QueueMeta meta = meta();

            // When
            boolean open = meta.isOpen(OPEN_AT);
            boolean beforeOpen = meta.isBeforeOpen(OPEN_AT);
            boolean closed = meta.isClosed(OPEN_AT);

            // Then
            assertThat(open).isTrue();
            assertThat(beforeOpen).isFalse();
            assertThat(closed).isFalse();
        }

        @Test
        @DisplayName("[T3-01] closeAt 직전(11:59:59.999)이면 열림 — isOpen=true, isBeforeOpen=false, isClosed=false")
        void justBeforeClose() {
            // Given
            QueueMeta meta = meta();

            // When
            boolean open = meta.isOpen(JUST_BEFORE_CLOSE);
            boolean beforeOpen = meta.isBeforeOpen(JUST_BEFORE_CLOSE);
            boolean closed = meta.isClosed(JUST_BEFORE_CLOSE);

            // Then
            assertThat(open).isTrue();
            assertThat(beforeOpen).isFalse();
            assertThat(closed).isFalse();
        }

        @Test
        @DisplayName("[T3-01] closeAt 정각(12:00)이면 마감 — isOpen=false, isBeforeOpen=false, isClosed=true")
        void exactlyCloseAt() {
            // Given
            QueueMeta meta = meta();

            // When
            boolean open = meta.isOpen(CLOSE_AT);
            boolean beforeOpen = meta.isBeforeOpen(CLOSE_AT);
            boolean closed = meta.isClosed(CLOSE_AT);

            // Then
            assertThat(open).isFalse();
            assertThat(beforeOpen).isFalse();
            assertThat(closed).isTrue();
        }
    }

    @Nested
    @DisplayName("생성 불변식 — openAt < closeAt, 필수 값, ms 내림")
    class Invariants {

        @Test
        @DisplayName("[T3-02] openAt 이 closeAt 보다 늦으면 DomainException")
        void openAfterClose() {
            // Given
            Instant openAt = CLOSE_AT;
            Instant closeAt = OPEN_AT;

            // When / Then
            assertThatThrownBy(() -> new QueueMeta(DROP_ID, openAt, closeAt))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] openAt 과 closeAt 이 같으면 DomainException")
        void openEqualsClose() {
            // Given
            Instant same = OPEN_AT;

            // When / Then
            assertThatThrownBy(() -> new QueueMeta(DROP_ID, same, same))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] dropId 가 null 이면 DomainException")
        void nullDropId() {
            // When / Then
            assertThatThrownBy(() -> new QueueMeta(null, OPEN_AT, CLOSE_AT))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] openAt 이 null 이면 DomainException")
        void nullOpenAt() {
            // When / Then
            assertThatThrownBy(() -> new QueueMeta(DROP_ID, null, CLOSE_AT))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] closeAt 이 null 이면 DomainException")
        void nullCloseAt() {
            // When / Then
            assertThatThrownBy(() -> new QueueMeta(DROP_ID, OPEN_AT, null))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] µs 시각(10:00:00.000999)은 ms 로 내림되어 openAt 이 10:00:00.000 이 된다")
        void truncatesMicrosToMillis() {
            // Given
            Instant openAtMicros = Instant.parse("2026-11-16T10:00:00.000999Z");
            Instant closeAtMicros = Instant.parse("2026-11-16T12:00:00.000999Z");

            // When
            QueueMeta meta = new QueueMeta(DROP_ID, openAtMicros, closeAtMicros);

            // Then
            assertThat(meta.openAt()).isEqualTo(Instant.parse("2026-11-16T10:00:00.000Z"));
            assertThat(meta.closeAt()).isEqualTo(Instant.parse("2026-11-16T12:00:00.000Z"));
            assertThat(meta.dropId()).isEqualTo(DROP_ID);
        }

        @Test
        @DisplayName("[T3-02] ms 내림 후 openAt 과 closeAt 이 같아지면(10:00:00.0001 / 10:00:00.0009) DomainException")
        void equalAfterTruncation() {
            // Given — 내림 전에는 openAt < closeAt 이지만, 내림 후 둘 다 10:00:00.000
            Instant openAt = Instant.parse("2026-11-16T10:00:00.0001Z");
            Instant closeAt = Instant.parse("2026-11-16T10:00:00.0009Z");

            // When / Then
            assertThatThrownBy(() -> new QueueMeta(DROP_ID, openAt, closeAt))
                    .isInstanceOf(DomainException.class);
        }

        @Test
        @DisplayName("[T3-02] 내림된 경계에서 isOpen 판정 — openAt 10:00:00.000999 로 생성해도 10:00:00.000 부터 열림")
        void openWindowUsesTruncatedOpenAt() {
            // Given
            QueueMeta meta = new QueueMeta(
                    DROP_ID, Instant.parse("2026-11-16T10:00:00.000999Z"), CLOSE_AT);

            // When
            boolean open = meta.isOpen(OPEN_AT);

            // Then
            assertThat(open).isTrue();
        }

        @Test
        @DisplayName("[T3-02] openAt 과 closeAt 이 1ms 차이면 생성된다 (최소 구간, 경계 off-by-one 방지)")
        void minimumWindowOneMilli() {
            // Given
            Instant closeAt = OPEN_AT.plusMillis(1);

            // When
            QueueMeta meta = new QueueMeta(DROP_ID, OPEN_AT, closeAt);

            // Then
            assertThat(meta.isOpen(OPEN_AT)).isTrue();
            assertThat(meta.isClosed(closeAt)).isTrue();
        }

        @Test
        @DisplayName("[T3-02] closeAt µs(12:00:00.000999)도 내림되어 12:00:00.000 정각에 마감으로 판정된다")
        void closeAtTruncatedClosesAtMilli() {
            // Given
            QueueMeta meta = new QueueMeta(
                    DROP_ID, OPEN_AT, Instant.parse("2026-11-16T12:00:00.000999Z"));

            // When
            boolean closed = meta.isClosed(CLOSE_AT);
            boolean open = meta.isOpen(CLOSE_AT);

            // Then
            assertThat(closed).isTrue();
            assertThat(open).isFalse();
        }
    }

    @Nested
    @DisplayName("EnqueueResult 팩토리")
    class EnqueueResultFactory {

        @Test
        @DisplayName("[T3-02] alreadyAdmitted() 는 admitted=true, rank=-1")
        void alreadyAdmitted() {
            // When
            EnqueueResult result = EnqueueResult.alreadyAdmitted();

            // Then
            assertThat(result.admitted()).isTrue();
            assertThat(result.rank()).isEqualTo(-1L);
        }

        @Test
        @DisplayName("[T3-02] waiting(3) 은 admitted=false, rank=3")
        void waiting() {
            // When
            EnqueueResult result = EnqueueResult.waiting(3);

            // Then
            assertThat(result.admitted()).isFalse();
            assertThat(result.rank()).isEqualTo(3L);
        }
    }
}
