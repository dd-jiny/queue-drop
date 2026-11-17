package com.queuedrop.coupon.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

/** 발행 실패 시 보상 (단계 문서 4장 T6-15, NFR-03). 전체 컨텍스트 + HTTP 로 503 매핑까지 확인한다 */
class CouponIssueFailureTest extends CouponScenarioTestBase {

    @Autowired MockMvc mockMvc;

    @Test
    @DisplayName("[T6-15] publisher 가 항상 예외면 503 COUPON_TEMPORARILY_FAILED, Redis 잔여·Set·requests 원상복구, 재요청도 같은 결과")
    void publishFailureCompensates() throws Exception {
        // Given — 수량 10, 발행이 항상 실패
        long eventId = givenOpenEvent(10);
        publisher.alwaysFail();

        // When / Then — 두 번 요청해도 매번 503 (보상이 DUPLICATE 를 남기지 않는다)
        for (int attempt = 0; attempt < 2; attempt++) {
            mockMvc.perform(post("/api/coupons/events/{eventId}/issue", eventId).header("X-User-Id", 5L))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.code").value("COUPON_TEMPORARILY_FAILED"));
        }

        // Then — Redis 원상복구, 발행된 것 없음, DB 없음
        assertThat(remaining(eventId)).isEqualTo("10");
        assertThat(isIssued(eventId, 5L)).isFalse();
        assertThat(issuedCount(eventId)).isZero();
        assertThat(requestOf(eventId, 5L)).isNull();
        assertThat(publisher.sentRequestIds(5L)).isEmpty();
        assertThat(dbCount(eventId)).isZero();
    }
}
