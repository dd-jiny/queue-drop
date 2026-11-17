package com.queuedrop.coupon.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.MyCouponResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate;
import com.queuedrop.coupon.application.service.fake.InMemoryCouponEventRepository;
import com.queuedrop.coupon.application.service.fake.InMemoryIssuedCouponRepository;
import com.queuedrop.coupon.domain.model.CouponEvent;
import com.queuedrop.coupon.domain.model.IssuedCoupon;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.transaction.annotation.Transactional;

/** CouponQueryService 단위 (Fake 포트). 전체 경로는 T6-19·T6-20 통합 테스트에서 다룬다. */
class CouponQueryServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-17T09:00:00Z");
    private static final Instant ISSUE_START_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final long USER_ID = 7L;

    private InMemoryCouponEventRepository eventRepository;
    private InMemoryIssuedCouponRepository issuedRepository;
    private FakeCouponGate gate;
    private CouponQueryService service;

    @BeforeEach
    void setUp() {
        eventRepository = new InMemoryCouponEventRepository();
        issuedRepository = new InMemoryIssuedCouponRepository();
        gate = new FakeCouponGate();
        service = new CouponQueryService(eventRepository, issuedRepository, gate);
    }

    private CouponEvent givenEvent(String name, int quantity) {
        return eventRepository.given(CouponEvent.create(name, quantity, ISSUE_START_AT, NOW));
    }

    @Nested
    @DisplayName("event")
    class Event {

        @Test
        @DisplayName("[T6-19 보강] 이벤트 정보 + Redis 잔여 수량을 반환한다")
        void returnsEventWithRemaining() {
            // Given
            CouponEvent event = givenEvent("오픈 기념 쿠폰", 3);
            gate.initialize(event.getId(), 3, ISSUE_START_AT);
            gate.tryIssue(event.getId(), 2L, "r2", ISSUE_START_AT);

            // When
            CouponEventResult result = service.event(event.getId());

            // Then
            assertThat(result).isEqualTo(new CouponEventResult(event.getId(), "오픈 기념 쿠폰", 3, 2, ISSUE_START_AT));
        }

        @Test
        @DisplayName("[T6-19 보강] 없는 이벤트면 EVENT_NOT_FOUND, Redis 를 읽지 않는다")
        void notFound() {
            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.event(999L))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CouponErrorCode.EVENT_NOT_FOUND));
            assertThat(gate.remainingCalls()).isEmpty();
        }

        @Test
        @DisplayName("[T6-19 보강] Redis 장애면 TEMPORARILY_FAILED(원인 GateUnavailableException) (v1.5)")
        void gateFailure() {
            // Given
            CouponEvent event = givenEvent("오픈 기념 쿠폰", 3);
            gate.failRemaining();

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.event(event.getId()))
                    .satisfies(e -> {
                        assertThat(e.errorCode()).isEqualTo(CouponErrorCode.TEMPORARILY_FAILED);
                        assertThat(e.getCause()).isInstanceOf(GateUnavailableException.class);
                    });
        }

        @Test
        @DisplayName("[T6-19 보강] event 는 트랜잭션을 열지 않는다 — DB 커넥션을 쥔 채 Redis 를 호출하지 않기 위해 (v1.5, readOnly 는 myCoupons 에만)")
        void eventHasNoTransaction() throws NoSuchMethodException {
            // Given
            var event = CouponQueryService.class.getMethod("event", long.class);
            var myCoupons = CouponQueryService.class.getMethod("myCoupons", Long.class);

            // When
            Transactional onEvent = AnnotatedElementUtils.findMergedAnnotation(event, Transactional.class);
            Transactional onClass = AnnotatedElementUtils.findMergedAnnotation(CouponQueryService.class, Transactional.class);
            Transactional onMyCoupons = AnnotatedElementUtils.findMergedAnnotation(myCoupons, Transactional.class);

            // Then
            assertThat(onEvent).as("event 메서드에 @Transactional 없음").isNull();
            assertThat(onClass).as("클래스 레벨 @Transactional 은 event 에도 걸리므로 없어야 한다").isNull();
            assertThat(onMyCoupons).as("myCoupons 는 readOnly 트랜잭션").isNotNull();
            assertThat(onMyCoupons.readOnly()).isTrue();
        }
    }

    @Nested
    @DisplayName("myCoupons")
    class MyCoupons {

        @Test
        @DisplayName("[T6-20 보강] 최신 발급순, 이벤트 이름 포함, 다른 사용자 쿠폰은 빠진다")
        void latestFirstWithEventName() {
            // Given
            CouponEvent first = givenEvent("오픈 기념 쿠폰", 10);
            CouponEvent second = givenEvent("주말 쿠폰", 10);
            Instant earlier = Instant.parse("2026-11-17T10:00:01Z");
            Instant later = Instant.parse("2026-11-17T10:00:02Z");
            issuedRepository.saveIfAbsent(IssuedCoupon.issue(first.getId(), USER_ID, earlier));
            issuedRepository.saveIfAbsent(IssuedCoupon.issue(second.getId(), USER_ID, later));
            issuedRepository.saveIfAbsent(IssuedCoupon.issue(first.getId(), 8L, later));

            // When
            List<MyCouponResult> result = service.myCoupons(USER_ID);

            // Then
            assertThat(result).containsExactly(
                    new MyCouponResult(second.getId(), "주말 쿠폰", later),
                    new MyCouponResult(first.getId(), "오픈 기념 쿠폰", earlier));
        }

        @Test
        @DisplayName("[T6-20 보강] 쿠폰이 없으면 빈 목록, Redis 를 읽지 않는다")
        void empty() {
            // When
            List<MyCouponResult> result = service.myCoupons(USER_ID);

            // Then
            assertThat(result).isEmpty();
            assertThat(gate.remainingCalls()).isEmpty();
        }
    }
}
