package com.queuedrop.entry.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/** 발행 실패 → 보상 시나리오 (단계 문서 4장 T4-30). HTTP 부터 Redis·Kafka·DB 까지 실제로 거친다 */
class EntryCompensationScenarioTest extends ScenarioTestBase {

    @Autowired MockMvc mockMvc;

    private ResultActions apply(long dropId, long userId) throws Exception {
        return mockMvc.perform(post("/api/entries/drops/{dropId}", dropId).header("X-User-Id", userId));
    }

    @Test
    @DisplayName("[T4-30] 발행이 실패하면 503 ENTRY_TEMPORARILY_FAILED, remaining·applicants 가 원상복구되고 같은 사용자 재응모는 202 (seq 는 재사용하지 않음)")
    void publishFailureIsCompensated() throws Exception {
        // Given — 정원 100 드로우, 사용자 토큰 보유, 다음 발행은 실패 (브로커에 보내지 않음)
        long dropId = 501L;
        long userId = 11L;
        givenOpenDrop(dropId, 100);
        givenToken(dropId, userId);
        publisher.failOnceFor(userId);

        // When — HTTP 응모
        apply(dropId, userId)
                // Then — 503 + 고정 에러 코드
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("ENTRY_TEMPORARILY_FAILED"));

        // Then — 보상: 잔여 정원·응모자 원상복구
        assertThat(remaining(dropId)).isEqualTo("100");
        assertThat(isApplicant(dropId, userId)).isFalse();
        assertThat(applicantCount(dropId)).isZero();

        // When — 같은 사용자 재응모 (정상 발행)
        apply(dropId, userId)
                // Then — 202, seq 는 보상된 1 을 재사용하지 않고 2
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.entrySeq").value(2));
        assertThat(remaining(dropId)).isEqualTo("99");
        assertThat(isApplicant(dropId, userId)).isTrue();

        // Then — 비동기 저장 후 DB 1건 (seq 2)
        awaitAllEntryEventsConsumed();
        assertThat(dbCount(dropId)).isEqualTo(1);
        assertThat(dbSeq(dropId, userId)).isEqualTo(2);
    }
}
