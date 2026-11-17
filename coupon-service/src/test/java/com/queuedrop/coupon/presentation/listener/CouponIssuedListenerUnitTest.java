package com.queuedrop.coupon.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.queuedrop.common.event.CouponIssued;
import com.queuedrop.common.event.Topics;
import com.queuedrop.coupon.application.dto.CouponSaveCommand;
import com.queuedrop.coupon.application.service.CouponSaveService;
import java.lang.reflect.Method;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * CouponIssuedListener 의 메시지 → 명령 변환 (커밋 #5 최소 동작).
 * Redis claim·재전달 멱등·DLT 는 통합 테스트 T6-17·T6-23·T6-29 (커밋 #6).
 */
class CouponIssuedListenerUnitTest {

    @Test
    @DisplayName("[T6-17 단위] CouponIssued 의 eventId(요청 ID)·couponEventId·userId·issuedAt 이 그대로 CouponSaveCommand 로 전달된다")
    void passesMessageToSaveCommand() {
        // Given
        CouponSaveService saveService = mock(CouponSaveService.class);
        CouponIssuedListener listener = new CouponIssuedListener(saveService);
        Instant issuedAt = Instant.parse("2026-11-20T10:30:00.123456Z");
        CouponIssued message = new CouponIssued("req-123", 1L, 2L, issuedAt);

        // When
        listener.on(message);

        // Then
        ArgumentCaptor<CouponSaveCommand> captor = ArgumentCaptor.forClass(CouponSaveCommand.class);
        verify(saveService).save(captor.capture());
        assertThat(captor.getValue()).isEqualTo(new CouponSaveCommand("req-123", 1L, 2L, issuedAt));
    }

    @Test
    @DisplayName("[T6-17 단위] coupon.issued 를 구독하고 groupId 는 공통 설정(spring.kafka.consumer.group-id)을 따른다 — id 가 그룹이 되지 않는다")
    void subscribesWithCommonGroupId() throws Exception {
        // Given
        Method on = CouponIssuedListener.class.getMethod("on", CouponIssued.class);

        // When
        KafkaListener annotation = on.getAnnotation(KafkaListener.class);

        // Then
        assertThat(annotation).as("@KafkaListener").isNotNull();
        assertThat(annotation.topics()).containsExactly(Topics.COUPON_ISSUED);
        assertThat(annotation.groupId()).isEmpty();
        if (!annotation.id().isEmpty()) {
            // id 를 두면(#6 pause 용) idIsGroup=false 여야 group 이 coupon-service 로 유지된다
            assertThat(annotation.idIsGroup()).isFalse();
        }
    }
}
