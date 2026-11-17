package com.queuedrop.coupon.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import ch.qos.logback.classic.Level;
import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.coupon.application.dto.CouponEventRegisterCommand;
import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.CouponReinitializeCommand;
import com.queuedrop.coupon.application.dto.Requester;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate.InitializeCall;
import com.queuedrop.coupon.application.service.fake.InMemoryCouponEventRepository;
import com.queuedrop.coupon.domain.exception.DomainException;
import com.queuedrop.coupon.domain.model.CouponEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CouponEventServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-17T09:00:00Z");
    private static final Instant ISSUE_START_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final Requester ADMIN = new Requester(1L, "ADMIN");
    private static final Requester USER = new Requester(2L, "USER");

    private InMemoryCouponEventRepository eventRepository;
    private FakeCouponGate gate;
    private CouponEventService service;
    private LogCapture logs;

    @BeforeEach
    void setUp() {
        eventRepository = new InMemoryCouponEventRepository();
        gate = new FakeCouponGate();
        service = new CouponEventService(eventRepository, gate, Clock.fixed(NOW, ZoneOffset.UTC));
        logs = new LogCapture(CouponEventService.class);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private static CouponEventRegisterCommand register(Requester requester) {
        return new CouponEventRegisterCommand(requester, "오픈 기념 쿠폰", 100, ISSUE_START_AT);
    }

    private static void assertErrorCode(ApplicationException e, Object expected) {
        assertThat(e.errorCode()).isEqualTo(expected);
    }

    static Stream<Arguments> nonAdmins() {
        return Stream.of(
                Arguments.of("USER", USER),
                Arguments.of("role null", new Requester(3L, null)),
                Arguments.of("requester null", null));
    }

    // ---------------------------------------------------------------- 등록

    @Nested
    @DisplayName("[T6-07] register")
    class Register {

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.queuedrop.coupon.application.service.CouponEventServiceTest#nonAdmins")
        @DisplayName("[T6-07] 관리자가 아니면 FORBIDDEN, 저장·초기화 없음")
        void forbidden(String label, Requester requester) {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.register(register(requester)))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
            assertThat(eventRepository.saveCalls()).isZero();
            assertThat(gate.initializeCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T6-07] 권한 검사가 도메인 검증보다 먼저다: USER 가 잘못된 값으로 등록해도 FORBIDDEN")
        void forbiddenBeforeValidation() {
            // Given
            CouponEventRegisterCommand invalid = new CouponEventRegisterCommand(USER, "", 0, NOW.minusSeconds(1));

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.register(invalid))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
        }

        @Test
        @DisplayName("[T6-07] 관리자 등록: DB 저장(createdAt=고정 시각) → 저장된 id·수량·시작 시각으로 Gate 초기화, 결과 remaining = totalQuantity")
        void registersAndInitializes() {
            // When
            CouponEventResult result = service.register(register(ADMIN));

            // Then
            CouponEvent saved = eventRepository.all().getFirst();
            assertThat(eventRepository.count()).isEqualTo(1);
            assertThat(saved.getCreatedAt()).isEqualTo(NOW);
            assertThat(gate.initializeCalls())
                    .containsExactly(new InitializeCall(saved.getId(), 100, ISSUE_START_AT));
            assertThat(result).isEqualTo(new CouponEventResult(saved.getId(), "오픈 기념 쿠폰", 100, 100, ISSUE_START_AT));
            assertThat(logs.messages(Level.WARN)).isEmpty();
        }

        @Test
        @DisplayName("[T6-07] Gate 초기화가 GateUnavailableException 이어도(remaining 도 실패) DB 저장은 유지되고 결과 반환(remaining = totalQuantity), WARN 1줄")
        void gateInitFailureStillReturnsResult() {
            // Given
            gate.failInitializeNext();
            gate.failRemaining();

            // When
            CouponEventResult result = service.register(register(ADMIN));

            // Then
            CouponEvent saved = eventRepository.all().getFirst();
            assertThat(eventRepository.count()).isEqualTo(1);
            assertThat(result.id()).isEqualTo(saved.getId());
            assertThat(result.remaining()).as("Redis 를 다시 읽지 않는다").isEqualTo(100);
            assertThat(result.totalQuantity()).isEqualTo(100);
            assertThat(gate.initializeCalls()).hasSize(1);
            assertThat(gate.isInitialized(saved.getId())).isFalse();
            assertThat(logs.events(Level.WARN)).singleElement().satisfies(event -> {
                assertThat(event.getFormattedMessage()).contains("couponEventId=" + saved.getId());
                assertThat(event.getThrowableProxy()).as("원인 예외 첨부").isNotNull();
            });
        }

        @Test
        @DisplayName("[T6-07] 등록 결과의 remaining 은 Redis 를 읽지 않는다 (정상 경로에서도 remaining 호출 없음)")
        void registerDoesNotReadRemaining() {
            // When
            service.register(register(ADMIN));

            // Then
            assertThat(gate.remainingCalls()).isEmpty();
        }

        static Stream<Arguments> invalidCommands() {
            return Stream.of(
                    Arguments.of("수량 0", new CouponEventRegisterCommand(ADMIN, "쿠폰", 0, ISSUE_START_AT)),
                    Arguments.of("빈 이름", new CouponEventRegisterCommand(ADMIN, " ", 100, ISSUE_START_AT)),
                    Arguments.of("시작 시각 = now", new CouponEventRegisterCommand(ADMIN, "쿠폰", 100, NOW)),
                    Arguments.of("시작 시각 null", new CouponEventRegisterCommand(ADMIN, "쿠폰", 100, null)));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("invalidCommands")
        @DisplayName("[T6-07] 도메인 검증 실패는 INVALID_EVENT(원인 DomainException), 저장·초기화 없음")
        void invalidEvent(String label, CouponEventRegisterCommand command) {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.register(command))
                    .satisfies(e -> {
                        assertErrorCode(e, CouponErrorCode.INVALID_EVENT);
                        assertThat(e.getCause()).isInstanceOf(DomainException.class);
                    });
            assertThat(eventRepository.saveCalls()).isZero();
            assertThat(gate.initializeCalls()).isEmpty();
        }
    }

    // ---------------------------------------------------------------- 재초기화

    @Nested
    @DisplayName("[T6-07] reinitialize")
    class Reinitialize {

        private CouponEvent givenEvent() {
            return eventRepository.given(CouponEvent.create("오픈 기념 쿠폰", 100, ISSUE_START_AT, NOW));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.queuedrop.coupon.application.service.CouponEventServiceTest#nonAdmins")
        @DisplayName("[T6-07] 관리자가 아니면 FORBIDDEN, 이벤트 조회 전에 거절(조회·초기화 없음)")
        void forbiddenBeforeLookup(String label, Requester requester) {
            // Given
            CouponEvent event = givenEvent();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.reinitialize(new CouponReinitializeCommand(requester, event.getId())))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
            assertThat(eventRepository.findByIdCalls()).isEmpty();
            assertThat(gate.initializeCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T6-07] USER 가 없는 이벤트를 요청해도 404 가 아니라 FORBIDDEN (존재 여부를 흘리지 않음)")
        void forbiddenForMissingEvent() {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.reinitialize(new CouponReinitializeCommand(USER, 999L)))
                    .satisfies(e -> assertErrorCode(e, CommonErrorCode.FORBIDDEN));
        }

        @Test
        @DisplayName("[T6-07] 관리자: 없는 이벤트면 EVENT_NOT_FOUND, 초기화 없음")
        void notFound() {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.reinitialize(new CouponReinitializeCommand(ADMIN, 999L)))
                    .satisfies(e -> assertErrorCode(e, CouponErrorCode.EVENT_NOT_FOUND));
            assertThat(gate.initializeCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T6-07] 관리자: 저장된 이벤트 값(id·수량·시작 시각)으로 Gate 를 초기화한다 (등록 시 초기화 실패 복구)")
        void reinitializesFromStoredEvent() {
            // Given
            CouponEvent event = givenEvent();

            // When
            service.reinitialize(new CouponReinitializeCommand(ADMIN, event.getId()));

            // Then
            assertThat(gate.initializeCalls())
                    .containsExactly(new InitializeCall(event.getId(), 100, ISSUE_START_AT));
            assertThat(gate.remainingOf(event.getId())).isEqualTo(100);
        }

        @Test
        @DisplayName("[T6-07] 관리자: 진행 중인 이벤트를 재초기화해도 잔여 수량을 덮어쓰지 않는다 (SET NX)")
        void reinitializeKeepsProgress() {
            // Given — 100개 중 1개 발급된 상태
            CouponEvent event = givenEvent();
            gate.initialize(event.getId(), 100, ISSUE_START_AT);
            gate.tryIssue(event.getId(), 7L, "r-1", ISSUE_START_AT);

            // When
            service.reinitialize(new CouponReinitializeCommand(ADMIN, event.getId()));

            // Then
            assertThat(gate.remainingOf(event.getId())).isEqualTo(99);
        }

        @Test
        @DisplayName("[T6-07] 관리자: Redis 장애면 서비스가 TEMPORARILY_FAILED(원인 GateUnavailableException)로 감싼다 (v1.5, 그대로 올리면 500)")
        void gateFailurePropagates() {
            // Given
            CouponEvent event = givenEvent();
            gate.failInitializeNext();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.reinitialize(new CouponReinitializeCommand(ADMIN, event.getId())))
                    .satisfies(e -> {
                        assertErrorCode(e, CouponErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isSameAs(gate.lastInitializeFailure());
                    });
            assertThat(gate.initializeCalls()).hasSize(1);
        }
    }
}
