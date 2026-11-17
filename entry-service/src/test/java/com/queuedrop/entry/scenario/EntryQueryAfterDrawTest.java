package com.queuedrop.entry.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.queuedrop.entry.application.service.DrawService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

/**
 * 추첨 후 조회 통합 (단계 문서 5단계 4장 T5-24, T5-23 통합 확인). 전체 entry 컨텍스트.
 * 추첨은 스케줄러와 같은 순서로 DrawService 를 직접 호출해 만든다 (조회만 검증하려고 스케줄러 구현과 분리).
 */
class EntryQueryAfterDrawTest extends ScenarioTestBase {

    @Autowired MockMvc mockMvc;
    @Autowired DrawService drawService;

    private void givenDrawnDrop(long dropId, int applicants, int winners) {
        givenDueDrop(dropId, applicants, winners);
        givenRedisApplicants(dropId, 1, applicants);
        givenSavedEntries(dropId, 1, applicants);
        drawService.closeDueDrops(ScenarioTestSupport.NOW);
        drawService.drawReadyDrops(ScenarioTestSupport.NOW);
        assertThat(snapshotStatus(dropId)).as("추첨 완료 전제").isEqualTo("DRAWN");
    }

    @Test
    @DisplayName("[T5-24] 추첨 완료 후 GET /me 는 당첨자 WON, 낙첨자 LOST 를 DB 결과대로 돌려준다")
    void myEntryShowsWonOrLost() throws Exception {
        // Given — 응모 10명, 당첨 3명 추첨 완료
        long dropId = 503L;
        givenDrawnDrop(dropId, 10, 3);
        Map<Long, String> dbStatus = new HashMap<>();
        jdbc.query("SELECT user_id, status FROM entries WHERE drop_id = ?",
                rs -> { dbStatus.put(rs.getLong(1), rs.getString(2)); }, dropId);

        // When — 응모자 10명이 각자 조회
        Map<Long, String> apiStatus = new HashMap<>();
        for (long userId = 1; userId <= 10; userId++) {
            String body = mockMvc.perform(get("/api/entries/drops/{dropId}/me", dropId).header("X-User-Id", userId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.dropId").value(dropId))
                    .andExpect(jsonPath("$.entrySeq").value((int) userId))
                    .andReturn().getResponse().getContentAsString();
            apiStatus.put(userId, JsonPath.read(body, "$.status"));
        }

        // Then
        assertThat(apiStatus).isEqualTo(dbStatus);
        assertThat(apiStatus.values().stream().filter("WON"::equals).count()).isEqualTo(3);
        assertThat(apiStatus.values().stream().filter("LOST"::equals).count()).isEqualTo(7);
    }

    @Test
    @DisplayName("[T5-24] 추첨 완료 후 GET /api/entries/me 목록에도 WON / LOST 가 보인다")
    void myEntriesListShowsResult() throws Exception {
        // Given
        long dropId = 504L;
        givenDrawnDrop(dropId, 2, 1);
        String expected = jdbc.queryForObject(
                "SELECT status FROM entries WHERE drop_id = ? AND user_id = 1", String.class, dropId);

        // When
        String body = mockMvc.perform(get("/api/entries/me").header("X-User-Id", 1L))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        // Then
        List<String> statuses = JsonPath.read(body, "$[?(@.dropId == " + dropId + ")].status");
        assertThat(statuses).containsExactly(expected);
        assertThat(expected).isIn("WON", "LOST");
    }

    @Test
    @DisplayName("[T5-23] 통합: 추첨 전 GET /result 는 409, 추첨 후 200 과 draw_results 의 seed 를 문자열로, 헤더 없이 조회된다")
    void resultEndToEnd() throws Exception {
        // Given — 마감 전(추첨 없음) 상태
        long dropId = 505L;

        // When / Then — 추첨 전
        mockMvc.perform(get("/api/entries/drops/{dropId}/result", dropId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ENTRY_DRAW_NOT_COMPLETED"));

        // Given — 추첨 완료
        givenDrawnDrop(dropId, 10, 3);
        long seed = jdbc.queryForObject("SELECT seed FROM draw_results WHERE drop_id = ?", Long.class, dropId);

        // When / Then — 추첨 후
        mockMvc.perform(get("/api/entries/drops/{dropId}/result", dropId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.dropId").value(dropId))
                .andExpect(jsonPath("$.applicantCount").value(10))
                .andExpect(jsonPath("$.winnerCount").value(3))
                .andExpect(jsonPath("$.seed").value(Long.toString(seed)))
                .andExpect(jsonPath("$.drawnAt").value(ScenarioTestSupport.NOW.toString()));
    }
}
