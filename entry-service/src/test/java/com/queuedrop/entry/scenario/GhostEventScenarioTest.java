package com.queuedrop.entry.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 유령 이벤트 시나리오 (단계 문서 4.4, T4-31; sequence-diagram.md 5.2).
 * "브로커는 기록했는데 ack 를 못 받은" 상황을, 실제로 보낸 뒤 실패를 던지는 Publisher 로 재현한다.
 * 모든 케이스에서 A 의 실패 응모(발행 → 보상)는 저장 리스너를 멈춘 채 실행한다 (순서 A).
 * 순서 B (T4-42): 리스너를 멈추지 않고, 컨슈머가 A 를 저장한 뒤에 발행 실패 → 보상 (E-40 회귀).
 * v1.5: 유령 이벤트 (A, seq 1, e1) 는 claim 에서 requests[A] 와 eventId 가 달라(또는 응모자가 아니라) 저장되지 않는다.
 */
class GhostEventScenarioTest extends ScenarioTestBase {

    private static final long USER_A = 1L;
    private static final long USER_B = 2L;
    private static final long USER_C = 3L;

    @Autowired MockMvc mockMvc;
    @Autowired KafkaListenerEndpointRegistry registry;

    @FunctionalInterface
    private interface Step {
        void run() throws Exception;
    }

    /** 저장 리스너를 멈춘 상태에서 step 을 실행하고 재개한다 */
    private void withSaverPaused(Step step) throws Exception {
        MessageListenerContainer saver = registry.getListenerContainer("entry-saver");
        saver.pause();
        try {
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                    .until(saver::isContainerPaused);
            step.run();
        } finally {
            saver.resume();
        }
    }

    private ResultActions apply(long dropId, long userId) throws Exception {
        return mockMvc.perform(post("/api/entries/drops/{dropId}", dropId).header("X-User-Id", userId));
    }

    /** A 응모: 브로커엔 (A, seq 1) 기록, 응답은 503, Redis 는 보상 */
    private void applyAWithAckLost(long dropId) throws Exception {
        publisher.loseAckOnceFor(USER_A);
        apply(dropId, USER_A)
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("ENTRY_TEMPORARILY_FAILED"));
        assertThat(isApplicant(dropId, USER_A)).isFalse();
        assertThat(remaining(dropId)).isEqualTo("100");
    }

    private void givenDropWithTokens(long dropId) {
        givenOpenDrop(dropId, 100);
        givenToken(dropId, USER_A);
        givenToken(dropId, USER_B);
    }

    @Test
    @DisplayName("[T4-31] 저장 리스너 정지 중 A 유령 이벤트(seq 1) → B(seq 2) → A 재응모(seq 3) 후 재개하면 DB 에 A 1건(seq 3), B 1건(seq 2), SCARD 2 == DB 2")
    void ghostEventBeforeReapply() throws Exception {
        // Given — 드로우(정원 100), A·B 토큰
        long dropId = 601L;
        givenDropWithTokens(dropId);

        // When — 리스너 정지 상태에서 A 유령 → B → A 재응모, 이후 재개 (설계 5.2 순서)
        withSaverPaused(() -> {
            applyAWithAckLost(dropId);
            apply(dropId, USER_B).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(2));
            apply(dropId, USER_A).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(3));
            assertThat(dbCount(dropId)).as("정지 중에는 저장되지 않는다").isZero();
        });

        // Then — 유령 이벤트(seq 1, e1)는 requests[A] = e3 와 달라 claim 0 → A 는 재응모 번호 seq 3 (v1.5)
        awaitAllEntryEventsConsumed();
        assertThat(dbSeq(dropId, USER_A)).isEqualTo(3);
        assertThat(dbSeq(dropId, USER_B)).isEqualTo(2);
        assertThat(dbCount(dropId)).isEqualTo(2);
        assertThat(applicantCount(dropId)).as("Redis 응모자 수 == DB 건수 (5단계 추첨 대기 없음)").isEqualTo(dbCount(dropId));
        assertThat(remaining(dropId)).isEqualTo("98");
    }

    @Test
    @DisplayName("[T4-31] (변형) 보상 직후 리스너를 재개하면 유령 이벤트가 A 재응모 전·후 어느 쪽에 처리돼도 A 는 seq 3, A·B 각 1건, 합계 2건, SCARD 2")
    void ghostEventResumedBeforeReapply() throws Exception {
        // Given
        long dropId = 602L;
        givenDropWithTokens(dropId);

        // When — A 유령 이벤트(보상까지 정지) → 재개 → B 응모 → A 재응모
        withSaverPaused(() -> applyAWithAckLost(dropId));
        apply(dropId, USER_B).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(2));
        apply(dropId, USER_A).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(3));

        // Then — 유령이 재응모 전에 처리되면 응모자 아님, 후에 처리되면 eventId 불일치로 claim 0 → 어느 쪽이든 seq 3 (v1.5)
        awaitAllEntryEventsConsumed();
        assertThat(dbSeq(dropId, USER_A)).isEqualTo(3);
        assertThat(dbSeq(dropId, USER_B)).isEqualTo(2);
        assertThat(dbCount(dropId)).isEqualTo(2);
        assertThat(applicantCount(dropId)).isEqualTo(2);
    }

    @Test
    @DisplayName("[T4-31] (변형) A 가 재응모하지 않으면 유령 이벤트는 무시되어 DB 는 B 1건, SCARD 1 로 일치한다")
    void ghostEventWithoutReapply() throws Exception {
        // Given
        long dropId = 603L;
        givenDropWithTokens(dropId);

        // When — A 유령 이벤트(보상까지 정지) → 재개 → B 응모, A 는 재응모하지 않음
        withSaverPaused(() -> applyAWithAckLost(dropId));
        apply(dropId, USER_B).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(2));

        // Then — 유령 이벤트까지 소비된 뒤에도 A 는 없다
        awaitAllEntryEventsConsumed();
        assertThat(dbSeq(dropId, USER_A)).isNull();
        assertThat(dbSeq(dropId, USER_B)).isEqualTo(2);
        assertThat(dbCount(dropId)).isEqualTo(1);
        assertThat(applicantCount(dropId)).isEqualTo(1);
    }

    @Test
    @DisplayName("[T4-42] 유령 이벤트가 보상보다 먼저 소비되면(정원 2) A 202, B 202, C 409 ENTRY_SOLD_OUT, DB 2건 == SCARD 2, remaining 0 — 정원 초과 없음")
    void ghostEventConsumedBeforeCompensation() throws Exception {
        // Given — 정원 2, A·B·C 토큰. 저장 리스너는 실행 중 그대로
        long dropId = 604L;
        givenOpenDrop(dropId, 2);
        givenToken(dropId, USER_A);
        givenToken(dropId, USER_B);
        givenToken(dropId, USER_C);
        publisher.consumeThenFailOnceFor(USER_A);

        // When — A: 발행 → 컨슈머 claim + INSERT → 발행 실패 → 보상 = ALREADY_PERSISTED
        apply(dropId, USER_A).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(1));
        apply(dropId, USER_B).andExpect(status().isAccepted()).andExpect(jsonPath("$.entrySeq").value(2));
        apply(dropId, USER_C)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ENTRY_SOLD_OUT"));

        // Then — DB 저장 ⇔ Redis 응모자 유지. v1.4 라면 A 의 자리가 풀려 C 가 성공하고 DB 3건(정원 + 1)
        awaitAllEntryEventsConsumed();
        assertThat(dbSeq(dropId, USER_A)).isEqualTo(1);
        assertThat(dbSeq(dropId, USER_B)).isEqualTo(2);
        assertThat(dbSeq(dropId, USER_C)).isNull();
        assertThat(dbCount(dropId)).as("정원 초과 없음").isEqualTo(2);
        assertThat(applicantCount(dropId)).as("Redis 응모자 수 == DB 건수").isEqualTo(dbCount(dropId));
        assertThat(remaining(dropId)).isEqualTo("0");
        assertThat(redis.opsForSet().members("entry:" + dropId + ":persisted"))
                .contains(String.valueOf(USER_A), String.valueOf(USER_B));
    }
}
