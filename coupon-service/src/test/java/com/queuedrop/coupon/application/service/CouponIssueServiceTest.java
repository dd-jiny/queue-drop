package com.queuedrop.coupon.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import ch.qos.logback.classic.Level;
import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.dto.CouponIssueResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.CouponIssuedPayload;
import com.queuedrop.coupon.application.port.EventPublishException;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.application.service.fake.FakeCouponEventPublisher;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class CouponIssueServiceTest {

    private static final long COUPON_EVENT_ID = 10L;
    private static final long USER_ID = 7L;
    private static final Instant ISSUE_START_AT = Instant.parse("2026-11-17T10:00:00Z");
    /** 발급 시작 후 고정 시각 */
    private static final Instant NOW = Instant.parse("2026-11-17T10:00:05Z");

    private FakeCouponGate gate;
    private FakeCouponEventPublisher publisher;
    private CouponIssueService service;
    private LogCapture logs;

    @BeforeEach
    void setUp() {
        gate = new FakeCouponGate();
        publisher = new FakeCouponEventPublisher();
        service = new CouponIssueService(gate, publisher, Clock.fixed(NOW, ZoneOffset.UTC));
        logs = new LogCapture(CouponIssueService.class);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private static CouponIssueCommand command() {
        return new CouponIssueCommand(COUPON_EVENT_ID, USER_ID);
    }

    /** 수량 quantity 로 초기화 (NOW 기준 발급 시작됨) */
    private void givenStartedEvent(int quantity) {
        gate.initialize(COUPON_EVENT_ID, quantity, ISSUE_START_AT);
    }

    /** tryIssue 가 정확히 한 번 호출됐고 그때 넘긴 requestId */
    private String singleTryIssueRequestId() {
        assertThat(gate.tryIssueCalls()).hasSize(1);
        return gate.tryIssueCalls().getFirst().requestId();
    }

    private void assertTemporarilyFailed(ApplicationException e, Class<? extends Throwable> causeType) {
        assertThat(e.errorCode()).isEqualTo(CouponErrorCode.TEMPORARILY_FAILED);
        assertThat(e.getCause()).isInstanceOf(causeType);
    }

    /** ERROR 로그 1줄, 식별자 3개 포함, 원인 예외 첨부 */
    private void assertSingleCompensationErrorLog(String requestId) {
        assertThat(logs.events(Level.ERROR)).singleElement().satisfies(event -> {
            assertThat(event.getFormattedMessage())
                    .contains("couponEventId=" + COUPON_EVENT_ID)
                    .contains("userId=" + USER_ID)
                    .contains("requestId=" + requestId);
            assertThat(event.getThrowableProxy()).as("보상 예외를 로그에 첨부").isNotNull();
        });
    }

    // ---------------------------------------------------------------- T6-03 성공

    @Test
    @DisplayName("[T6-03] Gate OK, 발행 성공이면 결과(couponEventId, issuedAt=고정 시각) 반환, 발행 1건(eventId=tryIssue 에 넘긴 UUID), 보상 없음")
    void issueOk() {
        // Given
        gate.nextOutcome(IssueOutcome.OK);

        // When
        CouponIssueResult result = service.issue(command());

        // Then
        assertThat(result.couponEventId()).isEqualTo(COUPON_EVENT_ID);
        assertThat(result.issuedAt()).isEqualTo(NOW);
        String requestId = singleTryIssueRequestId();
        assertThat(UUID.fromString(requestId)).as("requestId 는 UUID 형식").isNotNull();
        assertThat(publisher.published()).singleElement().satisfies(payload -> {
            assertThat(payload.eventId()).as("판정 전에 만든 요청 ID 가 그대로 발행된다").isEqualTo(requestId);
            assertThat(payload.couponEventId()).isEqualTo(COUPON_EVENT_ID);
            assertThat(payload.userId()).isEqualTo(USER_ID);
            assertThat(payload.issuedAt()).isEqualTo(NOW);
        });
        assertThat(gate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T6-03] 실제 판정 경로: 수량 3 중 1 사용, issued 등록, requests[userId] = 발행 eventId")
    void issueOkWithRealGate() {
        // Given
        givenStartedEvent(3);

        // When
        service.issue(command());

        // Then
        String publishedId = publisher.published().getFirst().eventId();
        assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
        assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(USER_ID);
        assertThat(gate.requests(COUPON_EVENT_ID)).containsExactly(Map.entry(USER_ID, publishedId));
    }

    @Test
    @DisplayName("[T6-03] 판정 시각은 주입된 Clock 이다: 시각이 issueStartAt 과 같으면 발급된다 (경계 포함)")
    void issuesExactlyAtStart() {
        // Given
        CouponIssueService atStart = new CouponIssueService(gate, publisher, Clock.fixed(ISSUE_START_AT, ZoneOffset.UTC));
        givenStartedEvent(3);

        // When
        CouponIssueResult result = atStart.issue(command());

        // Then
        assertThat(result.issuedAt()).isEqualTo(ISSUE_START_AT);
        assertThat(publisher.published()).hasSize(1);
    }

    @Test
    @DisplayName("[T6-03] 발급마다 requestId 가 다르고, tryIssue 와 발행 payload 의 순서·값이 같다")
    void requestIdUniquePerIssue() {
        // Given
        givenStartedEvent(3);

        // When
        service.issue(new CouponIssueCommand(COUPON_EVENT_ID, 1L));
        service.issue(new CouponIssueCommand(COUPON_EVENT_ID, 2L));

        // Then
        assertThat(publisher.published()).extracting(CouponIssuedPayload::eventId).doesNotHaveDuplicates();
        assertThat(gate.tryIssueCalls()).extracting(FakeCouponGate.Call::requestId)
                .containsExactlyElementsOf(publisher.published().stream().map(CouponIssuedPayload::eventId).toList());
    }

    // ---------------------------------------------------------------- T6-04 거절

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource({
            "NOT_STARTED, NOT_STARTED",
            "DUPLICATE, DUPLICATE",
            "SOLD_OUT, SOLD_OUT",
            "NOT_FOUND, EVENT_NOT_FOUND"
    })
    @DisplayName("[T6-04] Gate 가 거절하면 해당 CouponErrorCode 로 ApplicationException, 발행·보상 없음")
    void rejected(IssueOutcome outcome, CouponErrorCode expected) {
        // Given
        gate.nextOutcome(outcome);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.issue(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(expected));
        assertThat(publisher.attempted()).isEmpty();
        assertThat(gate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T6-04] 실제 판정 경로: 미초기화 이벤트면 EVENT_NOT_FOUND")
    void notInitializedWithRealGate() {
        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.issue(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.EVENT_NOT_FOUND));
        assertThat(publisher.attempted()).isEmpty();
    }

    @Test
    @DisplayName("[T6-04] 실제 판정 경로: 시작 1초 전이면 NOT_STARTED, 수량 변화 없음")
    void notStartedWithRealGate() {
        // Given
        CouponIssueService beforeStart = new CouponIssueService(
                gate, publisher, Clock.fixed(ISSUE_START_AT.minusSeconds(1), ZoneOffset.UTC));
        givenStartedEvent(3);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> beforeStart.issue(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.NOT_STARTED));
        assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(3);
        assertThat(publisher.attempted()).isEmpty();
    }

    @Test
    @DisplayName("[T6-04] 실제 판정 경로: 발급 후 같은 사용자가 다시 요청하면 DUPLICATE, 수량·발행 건수 변화 없음")
    void duplicateWithRealGate() {
        // Given
        givenStartedEvent(3);
        service.issue(command());

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.issue(command()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.DUPLICATE));
        assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
        assertThat(publisher.published()).hasSize(1);
        assertThat(gate.compensateCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T6-04] 실제 판정 경로: 수량 1 소진 후 다른 사용자는 SOLD_OUT")
    void soldOutWithRealGate() {
        // Given
        givenStartedEvent(1);
        service.issue(new CouponIssueCommand(COUPON_EVENT_ID, 1L));

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.issue(new CouponIssueCommand(COUPON_EVENT_ID, 2L)))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.SOLD_OUT));
        assertThat(gate.remainingOf(COUPON_EVENT_ID)).isZero();
        assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(1L);
        assertThat(publisher.published()).hasSize(1);
    }

    // ---------------------------------------------------------------- T6-05 발행 실패 → 보상 → 503

    @Nested
    @DisplayName("[T6-05] 발행 실패 → 같은 requestId 로 보상 1회 → TEMPORARILY_FAILED")
    class PublishFailed {

        @Test
        @DisplayName("[T6-05] 실제 판정 경로: 보상 COMPENSATED → 503, 수량·issued·requests 원상복구, ERROR 로그 없음")
        void compensated() {
            // Given
            givenStartedEvent(3);
            publisher.failNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, EventPublishException.class);
                        assertThat(e.getCause()).isSameAs(publisher.lastFailure());
                    });
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(gate.compensateResults()).containsExactly(CompensationResult.COMPENSATED);
            assertThat(publisher.attempted()).extracting(CouponIssuedPayload::eventId).containsExactly(requestId);
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(3);
            assertThat(gate.issued(COUPON_EVENT_ID)).isEmpty();
            assertThat(gate.requests(COUPON_EVENT_ID)).isEmpty();
            assertThat(publisher.published()).isEmpty();
            assertThat(logs.events(Level.ERROR)).isEmpty();
        }

        @Test
        @DisplayName("[T6-05] 보상 결과 NOT_OURS → 503, 같은 requestId 로 보상 1회, ERROR 로그 없음")
        void notOurs() {
            // Given
            gate.nextOutcome(IssueOutcome.OK);
            publisher.failNext();
            gate.nextCompensation(CompensationResult.NOT_OURS);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> assertTemporarilyFailed(e, EventPublishException.class));
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(publisher.attempted()).extracting(CouponIssuedPayload::eventId).containsExactly(requestId);
            assertThat(logs.events(Level.ERROR)).isEmpty();
        }

        @Test
        @DisplayName("[T6-05] 보상 자체가 예외 → 503(원인은 발행 예외, 보상 예외가 가리지 않음), 보상 1회, ERROR 로그 1줄")
        void compensateThrows() {
            // Given
            givenStartedEvent(3);
            publisher.failNext();
            gate.failCompensateNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, EventPublishException.class);
                        assertThat(e.getCause()).isSameAs(publisher.lastFailure());
                    });
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertSingleCompensationErrorLog(requestId);
            // 보상 실패: 수량 한 자리가 새는 쪽(언더 발급). 발행 0건이라 초과 발급은 없다
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
            assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(USER_ID);
            assertThat(publisher.published()).isEmpty();
        }

        @Test
        @DisplayName("[T6-05] 보상 후 같은 사용자가 다시 요청하면 새 requestId 로 발급되고 requests 는 새 ID 를 가리킨다")
        void retryAfterCompensation() {
            // Given
            givenStartedEvent(3);
            publisher.failNext();
            assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.issue(command()));

            // When
            service.issue(command());

            // Then
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
            assertThat(publisher.published()).hasSize(1);
            assertThat(gate.tryIssueCalls()).extracting(FakeCouponGate.Call::requestId)
                    .as("재요청은 새 요청 ID 를 쓴다").doesNotHaveDuplicates();
            assertThat(gate.requests(COUPON_EVENT_ID).get(USER_ID))
                    .isEqualTo(publisher.published().getFirst().eventId());
        }

        @Test
        @DisplayName("[T6-05] 수량 1 경계: 발행 실패로 보상된 마지막 자리는 다른 사용자가 받고, 그 다음 사용자는 SOLD_OUT (초과·누락 없음)")
        void compensatedLastSeatGoesToNextUser() {
            // Given
            long otherUser = 8L;
            long thirdUser = 9L;
            givenStartedEvent(1);
            publisher.failNext();
            assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.issue(command()));

            // When
            service.issue(new CouponIssueCommand(COUPON_EVENT_ID, otherUser));

            // Then
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isZero();
            assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(otherUser);
            assertThat(publisher.published()).extracting(CouponIssuedPayload::userId).containsExactly(otherUser);
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(new CouponIssueCommand(COUPON_EVENT_ID, thirdUser)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.SOLD_OUT));
            assertThat(publisher.published()).hasSize(1);
        }
    }

    // ---------------------------------------------------------------- T6-25 발행 실패 + ALREADY_PERSISTED → 성공

    @Nested
    @DisplayName("[T6-25] 발행 실패 + 보상 ALREADY_PERSISTED → 성공 (E-40)")
    class AlreadyPersisted {

        @Test
        @DisplayName("[T6-25] 보상 결과 ALREADY_PERSISTED 면 예외 없이 성공 결과(issuedAt=고정 시각), 보상 1회, WARN 1줄(식별자 3개), ERROR 없음")
        void alreadyPersistedIsSuccess() {
            // Given
            gate.nextOutcome(IssueOutcome.OK);
            publisher.failNext();
            gate.nextCompensation(CompensationResult.ALREADY_PERSISTED);

            // When
            CouponIssueResult result = service.issue(command());

            // Then
            assertThat(result.couponEventId()).isEqualTo(COUPON_EVENT_ID);
            assertThat(result.issuedAt()).isEqualTo(NOW);
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(publisher.attempted()).extracting(CouponIssuedPayload::eventId).containsExactly(requestId);
            assertThat(logs.messages(Level.WARN)).singleElement().satisfies(msg -> assertThat(msg)
                    .contains("couponEventId=" + COUPON_EVENT_ID)
                    .contains("userId=" + USER_ID)
                    .contains("requestId=" + requestId));
            assertThat(logs.events(Level.ERROR)).isEmpty();
        }

        @Test
        @DisplayName("[T6-25] 실제 판정 경로: 발행이 실패로 끝나기 전에 컨슈머가 같은 requestId 로 claim 했으면 보상이 거절되고 성공, 수량은 되돌리지 않는다")
        void consumerClaimedBeforeCompensation() {
            // Given — 메시지는 브로커에 기록돼 컨슈머가 저장 확정까지 했지만, 발행 응답은 실패로 끝난다
            givenStartedEvent(1);
            publisher.failNextAfter(payload -> assertThat(
                    gate.claimForSave(payload.couponEventId(), payload.userId(), payload.eventId())).isTrue());

            // When
            CouponIssueResult result = service.issue(command());

            // Then — 성공 = 기록. 수량이 되돌아가면 다른 사람이 받아 초과 발급이 된다
            assertThat(result.couponEventId()).isEqualTo(COUPON_EVENT_ID);
            assertThat(gate.compensateResults()).containsExactly(CompensationResult.ALREADY_PERSISTED);
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isZero();
            assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(USER_ID);
            assertThat(gate.persisted(COUPON_EVENT_ID)).containsExactly(USER_ID);
        }
    }

    // ---------------------------------------------------------------- T6-26 tryIssue 의 Gate 장애

    @Nested
    @DisplayName("[T6-26] tryIssue 가 GateUnavailableException → 같은 requestId 로 보상 후 TEMPORARILY_FAILED (E-41)")
    class GateUnavailable {

        @Test
        @DisplayName("[T6-26] ① Lua 실행 후 응답 타임아웃: 보상 COMPENSATED 로 수량·issued·requests 원복, 503(원인 = tryIssue 예외), 발행 없음")
        void timeoutAfterIssueCompensates() {
            // Given
            givenStartedEvent(3);
            gate.timeoutAfterIssueNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, GateUnavailableException.class);
                        assertThat(e.getCause()).isSameAs(gate.lastTryIssueFailure());
                    });
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(gate.compensateResults()).containsExactly(CompensationResult.COMPENSATED);
            assertThat(publisher.attempted()).isEmpty();
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(3);
            assertThat(gate.issued(COUPON_EVENT_ID)).isEmpty();
            assertThat(gate.requests(COUPON_EVENT_ID)).isEmpty();
            assertThat(logs.events(Level.ERROR)).isEmpty();
        }

        @Test
        @DisplayName("[T6-26] ② Lua 실행 전 실패: 보상 시도 → NOT_OURS, 상태 변화 없음, 503, 발행 없음")
        void failureBeforeIssueIsNotOurs() {
            // Given
            givenStartedEvent(3);
            gate.timeoutBeforeIssueNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, GateUnavailableException.class);
                        assertThat(e.getCause()).isSameAs(gate.lastTryIssueFailure());
                    });
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(gate.compensateResults()).containsExactly(CompensationResult.NOT_OURS);
            assertThat(publisher.attempted()).isEmpty();
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(3);
            assertThat(gate.issued(COUPON_EVENT_ID)).isEmpty();
            assertThat(logs.events(Level.ERROR)).isEmpty();
        }

        @Test
        @DisplayName("[T6-26] ③ 응답 타임아웃 후 보상도 실패: 503(원인 = tryIssue 예외, 보상 예외가 가리지 않음), 발행 없음, ERROR 로그 1줄")
        void compensateAlsoFails() {
            // Given
            givenStartedEvent(3);
            gate.timeoutAfterIssueNext();
            gate.failCompensateNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, GateUnavailableException.class);
                        assertThat(e.getCause()).isSameAs(gate.lastTryIssueFailure());
                    });
            String requestId = singleTryIssueRequestId();
            assertThat(gate.compensateCalls())
                    .containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, requestId));
            assertThat(publisher.attempted()).isEmpty();
            assertSingleCompensationErrorLog(requestId);
            // Redis 에만 발급이 남는다 (언더 발급 방향, 6장 리스크)
            assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(USER_ID);
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
        }

        @ParameterizedTest(name = "보상 결과 {0} → TEMPORARILY_FAILED")
        @EnumSource(CompensationResult.class)
        @DisplayName("[T6-26] Gate 장애 경로는 보상 결과와 무관하게 503 (ALREADY_PERSISTED 여도 성공으로 바꾸지 않는다)")
        void gateFailureIsAlways503(CompensationResult compensation) {
            // Given
            givenStartedEvent(3);
            gate.timeoutAfterIssueNext();
            gate.nextCompensation(compensation);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.issue(command()))
                    .satisfies(e -> {
                        assertTemporarilyFailed(e, GateUnavailableException.class);
                        assertThat(e.getCause()).isSameAs(gate.lastTryIssueFailure());
                    });
            assertThat(gate.compensateCalls()).hasSize(1);
            assertThat(publisher.attempted()).isEmpty();
        }

        @Test
        @DisplayName("[T6-26] Lua 미실행 장애 뒤 재요청하면 정상 발급된다")
        void retryAfterFailureBeforeIssue() {
            // Given
            givenStartedEvent(3);
            gate.timeoutBeforeIssueNext();
            assertThatExceptionOfType(ApplicationException.class).isThrownBy(() -> service.issue(command()));

            // When
            CouponIssueResult result = service.issue(command());

            // Then
            assertThat(result.couponEventId()).isEqualTo(COUPON_EVENT_ID);
            assertThat(gate.remainingOf(COUPON_EVENT_ID)).isEqualTo(2);
            assertThat(publisher.published()).hasSize(1);
        }
    }
}
