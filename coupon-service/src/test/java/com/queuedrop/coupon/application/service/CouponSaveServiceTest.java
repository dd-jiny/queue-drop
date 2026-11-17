package com.queuedrop.coupon.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import com.queuedrop.coupon.application.dto.CouponSaveCommand;
import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.GateResult;
import com.queuedrop.coupon.application.service.fake.FakeCouponGate;
import com.queuedrop.coupon.application.service.fake.InMemoryIssuedCouponRepository;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import com.queuedrop.coupon.domain.model.IssuedCoupon;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CouponSaveServiceTest {

    private static final long COUPON_EVENT_ID = 10L;
    private static final long USER_ID = 7L;
    private static final String REQUEST_ID = "r-1";
    private static final Instant ISSUE_START_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final Instant ISSUED_AT = Instant.parse("2026-11-17T10:00:05Z");

    private InMemoryIssuedCouponRepository issuedRepository;
    private FakeCouponGate gate;
    private CouponSaveService service;
    private LogCapture logs;

    @BeforeEach
    void setUp() {
        issuedRepository = new InMemoryIssuedCouponRepository();
        gate = new FakeCouponGate();
        service = new CouponSaveService(issuedRepository, gate);
        logs = new LogCapture(CouponSaveService.class);
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private static CouponSaveCommand command(String requestId) {
        return new CouponSaveCommand(requestId, COUPON_EVENT_ID, USER_ID, ISSUED_AT);
    }

    private void assertSingleGhostWarn(String requestId) {
        assertThat(logs.messages(Level.WARN)).singleElement().satisfies(msg -> assertThat(msg)
                .contains("couponEventId=" + COUPON_EVENT_ID)
                .contains("userId=" + USER_ID)
                .contains("requestId=" + requestId));
    }

    // ---------------------------------------------------------------- T6-06 claim false → 저장 안 함

    @Test
    @DisplayName("[T6-06] 보상으로 issued 에서 빠진 발급의 이벤트(유령)는 저장하지 않고 WARN 1줄, claim 에 command.eventId 를 넘긴다")
    void ignoresCompensatedIssue() {
        // Given — 실제 판정 → 보상
        gate.initialize(COUPON_EVENT_ID, 3, ISSUE_START_AT);
        assertThat(gate.tryIssue(COUPON_EVENT_ID, USER_ID, REQUEST_ID, ISSUED_AT))
                .isEqualTo(new GateResult(IssueOutcome.OK));
        assertThat(gate.compensate(COUPON_EVENT_ID, USER_ID, REQUEST_ID)).isEqualTo(CompensationResult.COMPENSATED);

        // When
        assertThatCode(() -> service.save(command(REQUEST_ID))).doesNotThrowAnyException();

        // Then
        assertThat(issuedRepository.saveCalls()).as("저장소를 호출하지 않는다").isZero();
        assertThat(issuedRepository.count()).isZero();
        assertThat(gate.claimCalls()).containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, REQUEST_ID));
        assertThat(gate.persisted(COUPON_EVENT_ID)).isEmpty();
        assertSingleGhostWarn(REQUEST_ID);
    }

    @Test
    @DisplayName("[T6-06] issued 에 있지만 requests 의 requestId 가 다르면(이전 시도의 유령 이벤트) 저장하지 않고 WARN 1줄")
    void ignoresEventOfOtherRequest() {
        // Given — 현재 발급 자리는 r-2 요청이 잡았다
        gate.givenIssued(COUPON_EVENT_ID, USER_ID, "r-2");

        // When
        assertThatCode(() -> service.save(command("r-1"))).doesNotThrowAnyException();

        // Then
        assertThat(issuedRepository.saveCalls()).isZero();
        assertThat(gate.claimCalls()).containsExactly(new FakeCouponGate.Call(COUPON_EVENT_ID, USER_ID, "r-1"));
        assertThat(gate.persisted(COUPON_EVENT_ID)).isEmpty();
        assertSingleGhostWarn("r-1");
    }

    @Test
    @DisplayName("[T6-06] claim 결과가 false 면(강제) 상태와 무관하게 저장소를 호출하지 않는다")
    void claimFalseNeverSaves() {
        // Given — Redis 상태로는 claim 이 통과할 발급이지만 claim 이 false 를 돌려준다
        gate.givenIssued(COUPON_EVENT_ID, USER_ID, REQUEST_ID);
        gate.nextClaim(false);

        // When
        service.save(command(REQUEST_ID));

        // Then
        assertThat(issuedRepository.saveCalls()).isZero();
        assertSingleGhostWarn(REQUEST_ID);
    }

    @Test
    @DisplayName("[T6-06] 판정(r1) → 보상(r1) → 재발급(r2) 후: r1 유령 이벤트는 무시되고 r2 이벤트만 저장된다 (초과 발급 없음)")
    void ghostOfPreviousAttemptIgnoredAfterReissue() {
        // Given — 실제 판정 경로
        gate.initialize(COUPON_EVENT_ID, 3, ISSUE_START_AT);
        gate.tryIssue(COUPON_EVENT_ID, USER_ID, "r1", ISSUED_AT);
        gate.compensate(COUPON_EVENT_ID, USER_ID, "r1");
        gate.tryIssue(COUPON_EVENT_ID, USER_ID, "r2", ISSUED_AT.plusSeconds(1));

        // When — 유령 이벤트가 먼저, 재발급 이벤트가 나중에 온다
        service.save(command("r1"));
        service.save(new CouponSaveCommand("r2", COUPON_EVENT_ID, USER_ID, ISSUED_AT.plusSeconds(1)));

        // Then
        assertThat(issuedRepository.count()).isEqualTo(1);
        assertThat(issuedRepository.find(COUPON_EVENT_ID, USER_ID).orElseThrow().getIssuedAt())
                .isEqualTo(ISSUED_AT.plusSeconds(1));
        assertThat(gate.persisted(COUPON_EVENT_ID)).containsExactly(USER_ID);
        assertSingleGhostWarn("r1");
    }

    // ---------------------------------------------------------------- T6-06 대조군: claim true → 저장

    @Test
    @DisplayName("[T6-06] (대조군) claim 이 통과하면 명령 값 그대로 저장되고 persisted 에 들어가며 WARN 이 없다")
    void savesClaimedIssue() {
        // Given
        gate.givenIssued(COUPON_EVENT_ID, USER_ID, REQUEST_ID);

        // When
        service.save(command(REQUEST_ID));

        // Then
        IssuedCoupon saved = issuedRepository.find(COUPON_EVENT_ID, USER_ID).orElseThrow();
        assertThat(saved.getCouponEventId()).isEqualTo(COUPON_EVENT_ID);
        assertThat(saved.getUserId()).isEqualTo(USER_ID);
        assertThat(saved.getIssuedAt()).isEqualTo(ISSUED_AT);
        assertThat(gate.persisted(COUPON_EVENT_ID)).containsExactly(USER_ID);
        assertThat(logs.messages(Level.WARN)).isEmpty();
    }

    @Test
    @DisplayName("[T6-06] (대조군) 같은 이벤트 재전달은 예외 없이 1건 유지, claim 도 두 번 다 통과, WARN 없음 (멱등)")
    void idempotentOnRedelivery() {
        // Given
        gate.givenIssued(COUPON_EVENT_ID, USER_ID, REQUEST_ID);
        service.save(command(REQUEST_ID));

        // When
        assertThatCode(() -> service.save(command(REQUEST_ID))).doesNotThrowAnyException();

        // Then
        assertThat(issuedRepository.count()).isEqualTo(1);
        assertThat(gate.claimCalls()).hasSize(2);
        assertThat(logs.messages(Level.WARN)).isEmpty();
    }

    @Test
    @DisplayName("[T6-06] (대조군) 저장 확정 뒤에는 같은 requestId 의 보상이 ALREADY_PERSISTED 로 거절되고 수량·issued 유지")
    void compensationRejectedAfterClaim() {
        // Given
        gate.initialize(COUPON_EVENT_ID, 1, ISSUE_START_AT);
        gate.tryIssue(COUPON_EVENT_ID, USER_ID, REQUEST_ID, ISSUED_AT);
        service.save(command(REQUEST_ID));

        // When — 발행 실패로 판단한 발급 서비스의 보상이 뒤늦게 도착
        CompensationResult compensation = gate.compensate(COUPON_EVENT_ID, USER_ID, REQUEST_ID);

        // Then
        assertThat(compensation).isEqualTo(CompensationResult.ALREADY_PERSISTED);
        assertThat(gate.remainingOf(COUPON_EVENT_ID)).isZero();
        assertThat(gate.issued(COUPON_EVENT_ID)).containsExactly(USER_ID);
        assertThat(issuedRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("[T6-06] (대조군) claim 통과 후 저장이 실패(DB 장애)하면 예외를 그대로 전파한다 — 재시도·DLT 대상, claim 은 재처리에도 true")
    void saveFailureAfterClaimPropagates() {
        // Given
        gate.givenIssued(COUPON_EVENT_ID, USER_ID, REQUEST_ID);
        RuntimeException dbDown = new IllegalStateException("simulated db down");
        issuedRepository.failSaveWith(dbDown);

        // When / Then
        assertThatThrownBy(() -> service.save(command(REQUEST_ID))).isSameAs(dbDown);
        assertThat(issuedRepository.count()).isZero();
        assertThat(gate.persisted(COUPON_EVENT_ID)).containsExactly(USER_ID);
        assertThat(gate.claimForSave(COUPON_EVENT_ID, USER_ID, REQUEST_ID)).as("재처리 시 claim 은 다시 true").isTrue();
    }
}
