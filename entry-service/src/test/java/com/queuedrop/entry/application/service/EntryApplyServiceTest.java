package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.EntryCreatedPayload;
import com.queuedrop.entry.application.port.EntryEventPublisher;
import com.queuedrop.entry.application.port.EventPublishException;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.application.service.fake.FakeEntryEventPublisher;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
    @DisplayName("[T4-03] Gate 가 OK(seq=7)면 결과 seq=7, payload 1건 발행(appliedAt=고정 시각, eventId=tryApply 에 넘긴 UUID), 보상 없음")
    void applyOk() {
        // Given
        entryGate.nextOutcome(GateResult.ok(7));

        // When
        EntryApplyResult result = service.apply(command());

        // Then
        assertThat(result.entrySeq()).isEqualTo(7);
        String gateEventId = singleTryApplyEventId();
        assertThat(UUID.fromString(gateEventId)).as("eventId 는 UUID 형식").isNotNull();
        assertThat(eventPublisher.published()).singleElement().satisfies(payload -> {
            assertThat(payload.dropId()).isEqualTo(DROP_ID);
            assertThat(payload.userId()).isEqualTo(USER_ID);
            assertThat(payload.entrySeq()).isEqualTo(7);
            assertThat(payload.appliedAt()).isEqualTo(NOW);
            assertThat(payload.eventId()).as("판정 전에 만든 요청 ID 가 그대로 발행된다").isEqualTo(gateEventId);
        });
        assertThat(entryGate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T4-03] 실제 판정 경로: requests[userId] 에 발행 payload 와 같은 eventId 가 기록된다")
    void applyOkRecordsRequestId() {
        // Given
        givenOpenDropWithToken();

        // When
        service.apply(command());

        // Then
        String publishedEventId = eventPublisher.published().getFirst().eventId();
        assertThat(entryGate.requests(DROP_ID)).containsExactly(Map.entry(USER_ID, publishedEventId));
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
        assertThat(entryGate.tryApplyCalls()).extracting(FakeEntryGate.Call::eventId)
                .containsExactlyElementsOf(eventPublisher.published().stream().map(EntryCreatedPayload::eventId).toList());
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
    @DisplayName("[T4-05] 발행 실패면 TEMPORARILY_FAILED, 같은 (dropId, userId, eventId) 로 보상 1회(COMPENSATED) → 정원·응모자·requests 원상복구")
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
        String eventId = singleTryApplyEventId();
        assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, eventId));
        assertThat(entryGate.compensateResults()).containsExactly(CompensationResult.COMPENSATED);
        assertThat(eventPublisher.attempted()).extracting(EntryCreatedPayload::eventId).containsExactly(eventId);
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
        assertThat(entryGate.applicants(DROP_ID)).isEmpty();
        assertThat(entryGate.requests(DROP_ID)).isEmpty();
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
        assertThat(entryGate.tryApplyCalls()).extracting(FakeEntryGate.Call::eventId)
                .as("재응모는 새 요청 ID 를 쓴다").doesNotHaveDuplicates();
        assertThat(entryGate.requests(DROP_ID).get(USER_ID)).isEqualTo(eventPublisher.published().getFirst().eventId());
    }

    @Test
    @DisplayName("[T4-06] 발행 실패 후 보상도 예외를 던져도 TEMPORARILY_FAILED 가 그대로 나간다")
    void compensateFailureDoesNotMaskOriginal() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();
        entryGate.failCompensateNext();

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.apply(command()))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                    assertThat(e.getCause()).isInstanceOf(EventPublishException.class);
                });
        assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, singleTryApplyEventId()));
    }

    @Test
    @DisplayName("[T4-06] 보상이 실패하면 자리는 응모자에 남는다 (언더 응모 쪽으로 새고, 발행 0건이라 초과 응모는 없다)")
    void compensateFailureLeavesSeatHeld() {
        // Given
        givenOpenDropWithToken();
        eventPublisher.failNext();
        entryGate.failCompensateNext();

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

    /** tryApply 가 정확히 한 번 호출됐고 그때 넘긴 eventId */
    private String singleTryApplyEventId() {
        assertThat(entryGate.tryApplyCalls()).hasSize(1);
        return entryGate.tryApplyCalls().getFirst().eventId();
    }

    @Nested
    @DisplayName("[T4-40] 발행 실패 + 보상 결과별 응답 (E-40)")
    class PublishFailedByCompensationResult {

        @Test
        @DisplayName("[T4-40] 보상 결과 ALREADY_PERSISTED 면 예외 없이 판정 seq=7 로 성공(202). 보상 1회, eventId 는 tryApply·payload 와 같다")
        void alreadyPersistedIsSuccess() {
            // Given
            entryGate.nextOutcome(GateResult.ok(7));
            eventPublisher.failNext();
            entryGate.nextCompensation(CompensationResult.ALREADY_PERSISTED);

            // When
            EntryApplyResult result = service.apply(command());

            // Then
            assertThat(result.entrySeq()).isEqualTo(7);
            String eventId = singleTryApplyEventId();
            assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, eventId));
            assertThat(eventPublisher.attempted()).singleElement().satisfies(payload -> {
                assertThat(payload.eventId()).isEqualTo(eventId);
                assertThat(payload.entrySeq()).isEqualTo(7);
            });
        }

        @ParameterizedTest(name = "{0} → TEMPORARILY_FAILED")
        @EnumSource(value = CompensationResult.class, names = {"COMPENSATED", "NOT_OURS"})
        @DisplayName("[T4-40] 보상 결과 COMPENSATED / NOT_OURS 면 TEMPORARILY_FAILED(503). 보상 1회, eventId 는 tryApply·payload 와 같다")
        void otherResultsFail(CompensationResult compensation) {
            // Given
            entryGate.nextOutcome(GateResult.ok(7));
            eventPublisher.failNext();
            entryGate.nextCompensation(compensation);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.apply(command()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isInstanceOf(EventPublishException.class);
                    });
            String eventId = singleTryApplyEventId();
            assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, eventId));
            assertThat(eventPublisher.attempted()).extracting(EntryCreatedPayload::eventId).containsExactly(eventId);
        }

        @Test
        @DisplayName("[T4-40] 실제 판정 경로: 발행이 실패로 끝나기 전에 컨슈머가 같은 eventId 로 claim 했으면 보상이 거절되고 202, 자리는 유지된다")
        void consumerClaimedBeforeCompensation() {
            // Given — 메시지는 브로커에 기록돼 컨슈머가 저장 확정까지 했지만, 발행 응답은 실패로 끝난다 (4.4 순서 B 의 단위판)
            givenOpenDropWithToken();
            EntryEventPublisher consumedThenFail = payload -> {
                assertThat(entryGate.claimForSave(payload.dropId(), payload.userId(), payload.eventId())).isTrue();
                throw new EventPublishException("ack 시간 초과 (fake)", new RuntimeException("simulated timeout"));
            };
            EntryApplyService consumedFirst = new EntryApplyService(
                    entryGate, consumedThenFail, Clock.fixed(NOW, ZoneOffset.UTC));

            // When
            EntryApplyResult result = consumedFirst.apply(command());

            // Then — 성공 = 기록 (불변식 유지), 정원 초과로 이어질 자리 해제 없음
            assertThat(result.entrySeq()).isEqualTo(1);
            assertThat(entryGate.compensateResults()).containsExactly(CompensationResult.ALREADY_PERSISTED);
            assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
            assertThat(entryGate.applicants(DROP_ID)).containsExactly(USER_ID);
            assertThat(entryGate.persisted(DROP_ID)).containsExactly(USER_ID);
        }
    }

    @Nested
    @DisplayName("[T4-41] Gate 장애 → 같은 eventId 로 보상 + 503 (E-41)")
    class GateUnavailable {

        @Test
        @DisplayName("[T4-41] 판정 후 응답 타임아웃이면 TEMPORARILY_FAILED, 발행 0, 같은 eventId 로 보상 1회(COMPENSATED) → 상태 원상복구")
        void timeoutAfterApplyCompensates() {
            // Given
            givenOpenDropWithToken();
            entryGate.timeoutAfterApplyNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.apply(command()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isInstanceOf(GateUnavailableException.class);
                    });
            String eventId = singleTryApplyEventId();
            assertThat(eventPublisher.attempted()).isEmpty();
            assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, eventId));
            assertThat(entryGate.compensateResults()).containsExactly(CompensationResult.COMPENSATED);
            assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
            assertThat(entryGate.applicants(DROP_ID)).isEmpty();
            assertThat(entryGate.requests(DROP_ID)).isEmpty();
            assertThat(entryGate.seq(DROP_ID)).as("seq 는 되돌리지 않는다").isEqualTo(1);
        }

        @Test
        @DisplayName("[T4-41] 판정 전에 실패(Lua 미실행)하면 TEMPORARILY_FAILED, 보상 1회 → NOT_OURS, 상태 변화 없음")
        void timeoutBeforeApplyIsNotOurs() {
            // Given
            givenOpenDropWithToken();
            entryGate.timeoutBeforeApplyNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.apply(command()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isInstanceOf(GateUnavailableException.class);
                    });
            String eventId = singleTryApplyEventId();
            assertThat(eventPublisher.attempted()).isEmpty();
            assertThat(entryGate.compensateCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, eventId));
            assertThat(entryGate.compensateResults()).containsExactly(CompensationResult.NOT_OURS);
            assertThat(entryGate.remaining(DROP_ID)).isEqualTo(3);
            assertThat(entryGate.applicants(DROP_ID)).isEmpty();
            assertThat(entryGate.seq(DROP_ID)).isZero();
        }

        @Test
        @DisplayName("[T4-41] tryApply 장애 후 보상 결과가 ALREADY_PERSISTED 여도 TEMPORARILY_FAILED (Gate 장애 경로는 보상 결과와 무관하게 503)")
        void gateFailureIsTemporarilyFailedEvenIfAlreadyPersisted() {
            // Given
            givenOpenDropWithToken();
            entryGate.timeoutAfterApplyNext();
            entryGate.nextCompensation(CompensationResult.ALREADY_PERSISTED);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.apply(command()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isSameAs(entryGate.lastTryApplyFailure());
                    });
            assertThat(entryGate.compensateCalls()).containsExactly(
                    new FakeEntryGate.Call(DROP_ID, USER_ID, singleTryApplyEventId()));
            assertThat(entryGate.compensateResults()).containsExactly(CompensationResult.ALREADY_PERSISTED);
            assertThat(eventPublisher.attempted()).isEmpty();
        }

        @Test
        @DisplayName("[T4-41] 응답 타임아웃 후 보상도 실패해도 TEMPORARILY_FAILED 이고 원인은 tryApply 의 예외다 (보상 예외가 가리지 않음)")
        void compensateFailureDoesNotMaskGateFailure() {
            // Given
            givenOpenDropWithToken();
            entryGate.timeoutAfterApplyNext();
            entryGate.failCompensateNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.apply(command()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(EntryErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isSameAs(entryGate.lastTryApplyFailure());
                    });
            assertThat(entryGate.compensateCalls()).containsExactly(
                    new FakeEntryGate.Call(DROP_ID, USER_ID, singleTryApplyEventId()));
            assertThat(eventPublisher.attempted()).isEmpty();
            // 보상 실패: Redis 에만 응모가 남는다 (6.3 리스크, D6 불일치 — 정원 초과는 아님)
            assertThat(entryGate.applicants(DROP_ID)).containsExactly(USER_ID);
            assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        }

        @Test
        @DisplayName("[T4-41] Lua 미실행 장애 뒤 재시도하면 정상 응모되고 seq=1 (장애가 번호를 소비하지 않음)")
        void retryAfterTimeoutBeforeApply() {
            // Given
            givenOpenDropWithToken();
            entryGate.timeoutBeforeApplyNext();
            assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.apply(command()));

            // When
            EntryApplyResult result = service.apply(command());

            // Then
            assertThat(result.entrySeq()).isEqualTo(1);
            assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
            assertThat(eventPublisher.published()).hasSize(1);
        }
    }
}
