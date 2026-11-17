package com.queuedrop.coupon.presentation.api;

import com.queuedrop.coupon.application.dto.CouponEventResult;
import com.queuedrop.coupon.application.dto.CouponReinitializeCommand;
import com.queuedrop.coupon.application.dto.Requester;
import com.queuedrop.coupon.application.service.CouponEventService;
import com.queuedrop.coupon.application.service.CouponQueryService;
import com.queuedrop.coupon.presentation.api.request.CouponEventRegisterRequest;
import com.queuedrop.coupon.presentation.api.response.CouponEventResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 쿠폰 이벤트 등록(ADMIN) · 조회(공개) · Redis 재초기화(ADMIN). 권한 판정은 application(AdminGuard) */
@RestController
@RequestMapping("/api/coupons/events")
@RequiredArgsConstructor
public class CouponEventController {

    private final CouponEventService eventService;
    private final CouponQueryService queryService;

    @PostMapping
    public ResponseEntity<CouponEventResponse> register(@RequestHeader("X-User-Id") Long userId,
                                                        @RequestHeader("X-User-Role") String role,
                                                        @Valid @RequestBody CouponEventRegisterRequest request) {
        CouponEventResult result = eventService.register(request.toCommand(new Requester(userId, role)));
        return ResponseEntity.status(HttpStatus.CREATED).body(CouponEventResponse.from(result));
    }

    @GetMapping("/{eventId:\\d+}")
    public CouponEventResponse get(@PathVariable Long eventId) {
        return CouponEventResponse.from(queryService.event(eventId));
    }

    @PostMapping("/{eventId:\\d+}/initialize")
    public ResponseEntity<Void> reinitialize(@RequestHeader("X-User-Id") Long userId,
                                             @RequestHeader("X-User-Role") String role,
                                             @PathVariable Long eventId) {
        eventService.reinitialize(new CouponReinitializeCommand(new Requester(userId, role), eventId));
        return ResponseEntity.noContent().build();
    }
}
