package com.queuedrop.coupon.presentation.api.request;

import com.queuedrop.coupon.application.dto.CouponEventRegisterCommand;
import com.queuedrop.coupon.application.dto.Requester;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

/** totalQuantity 는 Integer + @NotNull — 누락 시 toCommand 언박싱 NPE(500) 대신 400 */
public record CouponEventRegisterRequest(
        @NotBlank String name,
        @NotNull @Min(1) Integer totalQuantity,
        @NotNull @Future Instant issueStartAt) {

    public CouponEventRegisterCommand toCommand(Requester requester) {
        return new CouponEventRegisterCommand(requester, name, totalQuantity, issueStartAt);
    }
}
