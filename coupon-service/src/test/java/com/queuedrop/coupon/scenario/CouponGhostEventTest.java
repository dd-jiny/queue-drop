package com.queuedrop.coupon.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ch.qos.logback.classic.Level;
import com.queuedrop.coupon.support.CapturedLogs;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 유령 이벤트 시나리오 (단계 문서 4장 T6-16, T6-22, T6-27; E-40).
 * "브로커는 기록했는데 ack 를 못 받은" 상황을, 실제로 보낸 뒤 실패를 던지는 Publisher 로 재현한다.
 * 보상이 먼저(T6-16·T6-22): 저장 리스너(id {@value #SAVER_ID})를 멈춘 채 A 발급 → 보상까지 끝낸 뒤 재개.
 * 소비가 먼저(T6-27): 리스너를 멈추지 않고, 컨슈머가 A 를 저장한 뒤에 발행 실패 → 보상 = ALREADY_PERSISTED.
 */
class CouponGhostEventTest extends CouponScenarioTestBase {

    /** 저장 리스너 컨테이너 id (@KafkaListener(id = "coupon-saver", idIsGroup = false)) */
    static final String SAVER_ID = "coupon-saver";
    private static final long USER_A = 1L;
    private static final long USER_B = 2L;

    @Autowired MockMvc mockMvc;
    @Autowired KafkaListenerEndpointRegistry registry;

    @FunctionalInterface
    private interface Step {
        void run() throws Exception;
    }

    /** 저장 리스너를 멈춘 상태에서 step 을 실행하고 재개한다 (entry GhostEventScenarioTest.withSaverPaused 와 같음) */
    private void withSaverPaused(Step step) throws Exception {
        MessageListenerContainer saver = registry.getListenerContainer(SAVER_ID);
        assertThat(saver).as("저장 리스너가 id=%s 로 등록돼 있어야 pause 할 수 있다", SAVER_ID).isNotNull();
        saver.pause();
        try {
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                    .until(saver::isContainerPaused);
            step.run();
        } finally {
            saver.resume();
        }
    }

    private ResultActions issue(long eventId, long userId) throws Exception {
        return mockMvc.perform(post("/api/coupons/events/{eventId}/issue", eventId).header("X-User-Id", userId));
    }

    /** A 발급: 브로커엔 기록, 응답은 503, Redis 는 보상 */
    private void issueAWithAckLost(long eventId, String expectedRemaining) throws Exception {
        publisher.loseAckOnceFor(USER_A);
        issue(eventId, USER_A)
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("COUPON_TEMPORARILY_FAILED"));
        assertThat(publisher.sentRequestIds(USER_A)).as("유령 이벤트가 브로커에 기록됐다").hasSize(1);
        assertThat(isIssued(eventId, USER_A)).isFalse();
        assertThat(requestOf(eventId, USER_A)).isNull();
        assertThat(remaining(eventId)).isEqualTo(expectedRemaining);
    }

    @Test
    @DisplayName("[T6-16] (보상이 먼저) 수량 1, 리스너 정지 중 A 유령 이벤트 → 보상 → B 발급 후 재개하면 DB 에 B 1건만, A 없음, A 503 — 초과 발급 없음")
    void compensationBeforeConsumption() throws Exception {
        // Given — 수량 1
        long eventId = givenOpenEvent(1);

        try (CapturedLogs logs = new CapturedLogs("com.queuedrop.coupon")) {
            // When — 리스너 정지 상태에서 A(유령) → 보상 → B 성공, 이후 재개
            withSaverPaused(() -> {
                issueAWithAckLost(eventId, "1");
                issue(eventId, USER_B).andExpect(status().isAccepted());
                assertThat(dbCount(eventId)).as("정지 중에는 저장되지 않는다").isZero();
            });

            // Then — A 의 유령 이벤트는 requests 가 지워져 claim false → WARN 후 무시
            awaitAllIssuedEventsConsumed();
            String ghostRequestId = publisher.sentRequestIds(USER_A).getFirst();
            assertThat(logs.messages(Level.WARN))
                    .as("보상된 발급의 유령 이벤트는 claim false 로 무시된다")
                    .anyMatch(m -> m.contains(ghostRequestId));
        }
        assertThat(dbCount(eventId, USER_A)).isZero();
        assertThat(dbCount(eventId, USER_B)).isEqualTo(1);
        assertThat(dbCount(eventId)).as("초과 발급 없음").isEqualTo(1);
        assertThat(remaining(eventId)).isEqualTo("0");
        assertThat(isPersisted(eventId, USER_A)).isFalse();
        assertThat(isPersisted(eventId, USER_B)).isTrue();
    }

    @Test
    @DisplayName("[T6-22] 수량 2, 리스너 정지 중 A 유령 → 보상 → A 재발급(새 requestId) → B 발급 후 재개하면 DB 에 A·B 각 1건(총 2), 잔여 0, 이전 시도 유령은 claim false")
    void reissueAfterGhostEvent() throws Exception {
        // Given — 수량 2
        long eventId = givenOpenEvent(2);

        try (CapturedLogs logs = new CapturedLogs("com.queuedrop.coupon")) {
            // When — 리스너 정지 상태에서 A 유령 → 보상 → A 재발급 → B, 이후 재개
            withSaverPaused(() -> {
                issueAWithAckLost(eventId, "2");
                issue(eventId, USER_A).andExpect(status().isAccepted());
                issue(eventId, USER_B).andExpect(status().isAccepted());
                assertThat(dbCount(eventId)).as("정지 중에는 저장되지 않는다").isZero();
            });

            // Then — A 의 요청 ID 2개 (유령, 재발급) 중 Redis 에 남은 것은 재발급
            List<String> aRequests = publisher.sentRequestIds(USER_A);
            assertThat(aRequests).hasSize(2).doesNotHaveDuplicates();
            String ghostRequestId = aRequests.get(0);
            String reissueRequestId = aRequests.get(1);
            assertThat(requestOf(eventId, USER_A)).isEqualTo(reissueRequestId);

            // Then — 유령 이벤트(ghostRequestId)는 requests[A] 와 달라 claim false, 재발급만 저장
            awaitAllIssuedEventsConsumed();
            assertThat(dbCount(eventId, USER_A)).isEqualTo(1);
            assertThat(dbCount(eventId, USER_B)).isEqualTo(1);
            assertThat(dbCount(eventId)).as("총 2건 = 수량").isEqualTo(2);
            assertThat(remaining(eventId)).isEqualTo("0");
            assertThat(issuedCount(eventId)).isEqualTo(2);
            assertThat(logs.messages(Level.WARN))
                    .as("이전 시도의 유령 이벤트는 claim false 로 무시된다")
                    .anyMatch(m -> m.contains(ghostRequestId))
                    .noneMatch(m -> m.contains(reissueRequestId));
        }
    }

    @Test
    @DisplayName("[T6-27] (소비가 먼저) 수량 1, A 발행이 컨슈머 저장 완료 뒤에 실패하면 보상은 ALREADY_PERSISTED → A 202, B SOLD_OUT, DB A 1건, 잔여 0 — 초과 발급 없음")
    void consumptionBeforeCompensation() throws Exception {
        // Given — 수량 1, 저장 리스너는 실행 중 그대로
        long eventId = givenOpenEvent(1);
        publisher.consumeThenFailOnceFor(USER_A);

        // When — A: 발행 → 컨슈머 claim + INSERT → 발행 실패 → 보상 = ALREADY_PERSISTED → 성공
        issue(eventId, USER_A).andExpect(status().isAccepted());
        issue(eventId, USER_B)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("COUPON_SOLD_OUT"));

        // Then — 저장 확정된 A 의 자리는 되돌아가지 않는다
        awaitAllIssuedEventsConsumed();
        assertThat(dbCount(eventId, USER_A)).isEqualTo(1);
        assertThat(dbCount(eventId, USER_B)).isZero();
        assertThat(dbCount(eventId)).as("초과 발급 없음").isEqualTo(1);
        assertThat(remaining(eventId)).isEqualTo("0");
        assertThat(isIssued(eventId, USER_A)).isTrue();
        assertThat(isPersisted(eventId, USER_A)).isTrue();
    }
}
