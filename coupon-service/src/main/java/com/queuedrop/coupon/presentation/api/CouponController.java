package com.queuedrop.coupon.presentation.api;

import com.queuedrop.coupon.application.dto.CouponIssueCommand;
import com.queuedrop.coupon.application.dto.CouponIssueResult;
import com.queuedrop.coupon.application.service.CouponIssueService;
import com.queuedrop.coupon.application.service.CouponQueryService;
import com.queuedrop.coupon.presentation.api.response.CouponIssueResponse;
import com.queuedrop.coupon.presentation.api.response.MyCouponResponse;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 선착순 발급(202, 저장은 비동기) · 내 쿠폰 목록(DB 기준 — 발급 직후엔 비어 있을 수 있다, D6-3) */
@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {

    private final CouponIssueService issueService;
    private final CouponQueryService queryService;

    @PostMapping("/events/{eventId:\\d+}/issue")
    public ResponseEntity<CouponIssueResponse> issue(@RequestHeader("X-User-Id") Long userId,
                                                     @PathVariable Long eventId) {
        CouponIssueResult result = issueService.issue(new CouponIssueCommand(eventId, userId));
        return ResponseEntity.accepted().body(CouponIssueResponse.from(result));
    }

    @GetMapping("/me")
    public List<MyCouponResponse> me(@RequestHeader("X-User-Id") Long userId) {
        return queryService.myCoupons(userId).stream().map(MyCouponResponse::from).toList();
    }
}
