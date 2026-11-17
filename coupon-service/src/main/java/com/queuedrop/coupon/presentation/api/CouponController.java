package com.queuedrop.coupon.presentation.api;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.dto.CouponIssueResult;
import com.queuedrop.coupon.application.exception.CouponErrorCode;
import com.queuedrop.coupon.application.service.CouponIssueService;
import com.queuedrop.coupon.application.service.CouponQueryService;
import com.queuedrop.coupon.presentation.api.response.CouponIssueResponse;
import com.queuedrop.coupon.presentation.api.response.MyCouponResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 선착순 발급(202, 저장은 비동기) · 내 쿠폰 목록(DB 기준 — 발급 직후엔 비어 있을 수 있다, D6-3).
 * 발급 판정 결과는 coupon.issue.outcome{outcome} 로 센다 — 태그는 IssueOutcome 상수명 + TEMPORARILY_FAILED (8단계 3.2).
 */
@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {

    private static final String ISSUE_OUTCOME = "coupon.issue.outcome";

    private final CouponIssueService issueService;
    private final CouponQueryService queryService;
    private final MeterRegistry meterRegistry;

    @PostMapping("/events/{eventId:\\d+}/issue")
    public ResponseEntity<CouponIssueResponse> issue(@RequestHeader("X-User-Id") Long userId,
                                                     @PathVariable Long eventId) {
        CouponIssueResult result;
        try {
            result = issueService.issue(new CouponIssueCommand(eventId, userId));
        } catch (ApplicationException e) {
            if (e.errorCode() instanceof CouponErrorCode code) {
                countOutcome(outcomeTag(code));
            }
            throw e;
        }
        countOutcome("OK");
        return ResponseEntity.accepted().body(CouponIssueResponse.from(result));
    }

    @GetMapping("/me")
    public List<MyCouponResponse> me(@RequestHeader("X-User-Id") Long userId) {
        return queryService.myCoupons(userId).stream().map(MyCouponResponse::from).toList();
    }

    private void countOutcome(String outcome) {
        meterRegistry.counter(ISSUE_OUTCOME, "outcome", outcome).increment();
    }

    /** 태그는 판정 이름: Redis meta 없음(EVENT_NOT_FOUND)은 IssueOutcome 상수명 NOT_FOUND 로 */
    private static String outcomeTag(CouponErrorCode code) {
        return code == CouponErrorCode.EVENT_NOT_FOUND ? "NOT_FOUND" : code.name();
    }
}
