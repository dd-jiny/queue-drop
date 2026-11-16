package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EventPublishException;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.service.fake.FakeEntryEventPublisher;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class EntryApplyServiceTest {

    private static final long DROP_ID = 10L;
    private static final long USER_ID = 7L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");
    /** 응모 기간 중 고정 시각 */
    private static final Instant NOW = Instant.parse("2026-11-16T10:30:00Z");

    private FakeEntryGate entryGate;
    private FakeEntryEventPublisher eventPublisher;
    private EntryApplyService service;

    @BeforeEach
    void setUp() {
        entryGate = new FakeEntryGate();
        eventPublisher = new FakeEntryEventPublisher();
        service = new EntryApplyService(entryGate, eventPublisher, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static EntryApplyCommand command() {
        return new EntryApplyCommand(DROP_ID, USER_ID);
    }

    /** 정원 3, 기간 중, USER_ID 토큰 보유 */
    private void givenOpenDropWithToken() {
        entryGate.initialize(DROP_ID, 3, OPEN_AT, CLOSE_AT);
        entryGate.grantToken(DROP_ID, USER_ID);
    }

    @Test
    @DisplayName("[T4-03] Gate 가 OK(seq=7)면 결과 seq=7, payload 1건 발행(appliedAt=고정 시각), 보상 없음")
    void applyOk() {
        // Given
        entryGate.nextOutcome(GateResult.ok(7));

        // When
        EntryApplyResult result = service.apply(command());

        // Then
        assertThat(result.entrySeq()).isEqualTo(7);
        assertThat(eventPublisher.published()).singleElement().satisfies(payload -> {
            assertThat(payload.dropId()).isEqualTo(DROP_ID);
            assertThat(payload.userId()).isEqualTo(USER_ID);
            assertThat(payload.entrySeq()).isEqualTo(7);
            assertThat(payload.appliedAt()).isEqualTo(NOW);
            assertThat(UUID.fromString(payload.eventId())).isNotNull();
        });
        assertThat(entryGate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T4-03] 실제 판정 경로: 정원 3 중 1자리 사용, 응모자 등록, seq=1")
    void applyOkWithRealGate() {
        // Given
        givenOpenDropWithToken();

        // When
        EntryApplyResult result = service.apply(command());

        // Then
        assertThat(result.entrySeq()).isEqualTo(1);
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        assertThat(entryGate.applicants(DROP_ID)).containsExactly(USER_ID);
        assertThat(eventPublisher.published()).extracting(EntryCreatedPayload::entrySeq).containsExactly(1);
    }

    @Test
    @DisplayName("[T4-03] 응모마다 eventId 가 다르다")
    void eventIdUniquePerApply() {
        // Given
        entryGate.nextOutcome(GateResult.ok(1));
        entryGate.nextOutcome(GateResult.ok(2));

        // When
        service.apply(new EntryApplyCommand(DROP_ID, 1L));
        service.apply(new EntryApplyCommand(DROP_ID, 2L));

        // Then
        assertThat(eventPublisher.published()).extracting(EntryCreatedPayload::eventId).doesNotHaveDuplicates();
    }

    @ParameterizedTest(name = "{0} → EntryErrorCode.{0}")
    @EnumSource(value = ApplyOutcome.class, names = "OK", mode = EnumSource.Mode.EXCLUDE)
    @DisplayName("[T4-04] Gate 가 거절하면 같은 이름의 EntryErrorCode 로 ApplicationException, 발행·보상 없음")
    void rejected(ApplyOutcome outcome) {
        // Given
        entryGate.nextOutcome(GateResult.rejected(outcome));

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(EntryErrorCode.valueOf(outcome.name())));
        assertThat(eventPublisher.published()).isEmpty();
        assertThat(entryGate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T4-04] 실제 판정 경로: 토큰 없으면 NO_TOKEN, 정원 변화 없음")
    void noTokenWithRealGate() {
        // Given — 토큰 없음
        entryGate.initialize(DROP_ID, 3, OPEN_AT, CLOSE_AT);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(EntryErrorCode.NO_TOKEN));
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
        assertThat(eventPublisher.published()).isEmpty();
    }

    @Test
    @DisplayName("[T4-05] 발행 실패면 TEMPORARILY_FAILED, 같은 (dropId, userId) 로 보상 1회 → 정원·응모자 원상복구")
    void publishFailedCompensates() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                    assertThat(e.getCause()).isInstanceOf(EventPublishException.class);
                });
        assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID));
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
        assertThat(entryGate.applicants(DROP_ID)).isEmpty();
        assertThat(eventPublisher.published()).isEmpty();
    }

    @Test
    @DisplayName("[T4-05] 보상 후 같은 사용자가 재응모하면 성공하고 seq 는 재사용하지 않는다")
    void retryAfterCompensation() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();
        assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.apply(command()));

        // When
        EntryApplyResult result = service.apply(command());

        // Then
        assertThat(result.entrySeq()).isEqualTo(2);
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        assertThat(eventPublisher.published()).hasSize(1);
    }

    @Test
    @DisplayName("[T4-06] 발행 실패 후 보상도 예외를 던져도 TEMPORARILY_FAILED 가 그대로 나간다")
    void compensateFailureDoesNotMaskOriginal() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();
        entryGate.failCompensateWith(new IllegalStateException("redis down"));

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                    assertThat(e.getCause()).isInstanceOf(EventPublishException.class);
                });
        assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID));
    }
    @Test
    @DisplayName("[T4-06] 보상이 실패하면 자리는 응모자에 남는다 (언더 응모 쪽으로 새고, 발행 0건이라 초과 응모는 없다)")
    void compensateFailureLeavesSeatHeld() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();
        entryGate.failCompensateWith(new IllegalStateException("redis down"));

        // When
        assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.apply(command()));

        // Then
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        assertThat(entryGate.applicants(DROP_ID)).containsExactly(USER_ID);
        assertThat(eventPublisher.published()).isEmpty();
    }

    @Test
    @DisplayName("[T4-04] 실제 판정 경로: 성공 후 같은 사용자가 다시 응모하면 DUPLICATE, 정원·발행 건수 변화 없음")
    void duplicateAfterSuccessWithRealGate() {
        // Given
        givenOpenDropWithToken();
        service.apply(command());

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(EntryErrorCode.DUPLICATE));
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        assertThat(eventPublisher.published()).hasSize(1);
        assertThat(entryGate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T4-04] 판정 시각은 주입된 Clock 이다: 시각이 마감(closeAt)과 같으면 CLOSED")
    void usesInjectedClockForGate() {
        // Given — 마감 시각에 고정된 시계
        EntryApplyService atClose = new EntryApplyService(entryGate, eventPublisher, Clock.fixed(CLOSE_AT, ZoneOffset.UTC));
        givenOpenDropWithToken();

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> atClose.apply(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(EntryErrorCode.CLOSED));
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
        assertThat(eventPublisher.published()).isEmpty();
    }
}
