package com.queuedrop.ranking.presentation.api;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.ranking.application.dto.DropRankResult;
import com.queuedrop.ranking.application.dto.MyRankResult;
import com.queuedrop.ranking.application.dto.UserRankResult;
import com.queuedrop.ranking.application.dto.WeeklyRankingResult;
import com.queuedrop.ranking.application.exception.RankingErrorCode;
import com.queuedrop.ranking.application.service.RankingQueryService;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 랭킹 조회 API 슬라이스 (단계 문서 3.4, T7-19, T7-27(controller)).
 * domain 타입(WeekKey, RankEntry, RankingStoreUnavailableException 등)은 import 하지 않는다.
 * size·week 검증은 서비스 몫이므로 컨트롤러는 값을 그대로 넘기고, 서비스가 던진 ApplicationException 의 매핑만 확인한다.
 */
@WebMvcTest(RankingController.class)
@Import(GlobalExceptionHandler.class)
class RankingControllerTest {

    private static final String DROPS = "/api/rankings/drops";
    private static final String WEEKLY = "/api/rankings/users/weekly";
    private static final String ALL = "/api/rankings/users/all";
    private static final String ME = "/api/rankings/users/me";

    @Autowired MockMvc mockMvc;
    @MockitoBean RankingQueryService queryService;

    private static final List<UserRankResult> USERS = List.of(
            new UserRankResult(1, 9L, "nine", 5),
            new UserRankResult(2, 10L, "user-10", 5));

    private static final String USERS_JSON = """
            [{"rank":1,"userId":9,"nickname":"nine","score":5},
             {"rank":2,"userId":10,"nickname":"user-10","score":5}]""";

    private ResultActions expectError(ResultActions result, RankingErrorCode code) throws Exception {
        return result.andExpect(status().is(code.status()))
                .andExpect(jsonPath("$.code").value(code.code()))
                .andExpect(jsonPath("$.message").value(code.message()));
    }

    @Nested
    @DisplayName("GET /api/rankings/drops — 인기 드로우")
    class Drops {

        @Test
        @DisplayName("[T7-19] size 를 생략하면 10 으로 조회하고, 200 과 {rank, dropId, title, entryCount} 배열을 서비스 순서 그대로 응답한다")
        void defaultSize() throws Exception {
            // Given
            given(queryService.topDrops(10)).willReturn(List.of(
                    new DropRankResult(1, 3L, "한정판 A", 1000),
                    new DropRankResult(2, 7L, "drop-7", 20)));

            // When
            ResultActions result = mockMvc.perform(get(DROPS));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            [{"rank":1,"dropId":3,"title":"한정판 A","entryCount":1000},
                             {"rank":2,"dropId":7,"title":"drop-7","entryCount":20}]""", JsonCompareMode.STRICT));
            verify(queryService).topDrops(10);
        }

        @Test
        @DisplayName("[T7-19] size 를 지정하면 그 값을 서비스에 넘기고, 결과가 없으면 200 과 빈 배열")
        void explicitSizeEmpty() throws Exception {
            // Given
            given(queryService.topDrops(5)).willReturn(List.of());

            // When
            ResultActions result = mockMvc.perform(get(DROPS).param("size", "5"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("[]", JsonCompareMode.STRICT));
            verify(queryService).topDrops(5);
        }

        @Test
        @DisplayName("[T7-19] ?size=0 → 서비스가 INVALID_SIZE → 400 RANKING_INVALID_SIZE")
        void invalidSize() throws Exception {
            // Given
            given(queryService.topDrops(0)).willThrow(new ApplicationException(RankingErrorCode.INVALID_SIZE));

            // When
            ResultActions result = mockMvc.perform(get(DROPS).param("size", "0"));

            // Then
            expectError(result, RankingErrorCode.INVALID_SIZE);
        }

        @Test
        @DisplayName("[T7-27] Redis 장애로 서비스가 TEMPORARILY_FAILED → 503 RANKING_TEMPORARILY_FAILED")
        void unavailable() throws Exception {
            // Given
            given(queryService.topDrops(anyInt())).willThrow(new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED));

            // When
            ResultActions result = mockMvc.perform(get(DROPS));

            // Then
            expectError(result, RankingErrorCode.TEMPORARILY_FAILED);
        }

        @ParameterizedTest(name = "size={0}")
        @ValueSource(strings = {"abc", "1.5"})
        @DisplayName("[T7-19 보강] 숫자가 아닌 size 는 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void nonNumericSize(String size) throws Exception {
            // When
            ResultActions result = mockMvc.perform(get(DROPS).param("size", size));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(queryService);
        }
    }

    @Nested
    @DisplayName("GET /api/rankings/users/weekly — 주간 유저")
    class Weekly {

        @Test
        @DisplayName("[T7-19] week·size 를 그대로 넘기고 200 과 {week, items[{rank, userId, nickname, score}]}")
        void withWeek() throws Exception {
            // Given
            given(queryService.weeklyUsers("2026-W47", 5)).willReturn(new WeeklyRankingResult("2026-W47", USERS));

            // When
            ResultActions result = mockMvc.perform(get(WEEKLY).param("week", "2026-W47").param("size", "5"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("{\"week\":\"2026-W47\",\"items\":" + USERS_JSON + "}",
                            JsonCompareMode.STRICT));
            verify(queryService).weeklyUsers("2026-W47", 5);
        }

        @Test
        @DisplayName("[T7-19] week·size 를 생략하면 서비스에 null·10 을 넘기고, 서비스가 정한 이번 주 week 를 응답한다")
        void defaultWeekAndSize() throws Exception {
            // Given
            given(queryService.weeklyUsers(isNull(), anyInt())).willReturn(new WeeklyRankingResult("2026-W47", List.of()));

            // When
            ResultActions result = mockMvc.perform(get(WEEKLY));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"week":"2026-W47","items":[]}""", JsonCompareMode.STRICT));
            verify(queryService).weeklyUsers(null, 10);
        }

        @Test
        @DisplayName("[T7-19] ?week=2026-W99 → 서비스가 INVALID_WEEK → 400 RANKING_INVALID_WEEK")
        void invalidWeek() throws Exception {
            // Given
            given(queryService.weeklyUsers("2026-W99", 10))
                    .willThrow(new ApplicationException(RankingErrorCode.INVALID_WEEK));

            // When
            ResultActions result = mockMvc.perform(get(WEEKLY).param("week", "2026-W99"));

            // Then
            expectError(result, RankingErrorCode.INVALID_WEEK);
        }

        @Test
        @DisplayName("[T7-19] ?size=101 → 서비스가 INVALID_SIZE → 400 RANKING_INVALID_SIZE")
        void invalidSize() throws Exception {
            // Given
            given(queryService.weeklyUsers(isNull(), anyInt()))
                    .willThrow(new ApplicationException(RankingErrorCode.INVALID_SIZE));

            // When
            ResultActions result = mockMvc.perform(get(WEEKLY).param("size", "101"));

            // Then
            expectError(result, RankingErrorCode.INVALID_SIZE);
            verify(queryService).weeklyUsers(null, 101);
        }

        @Test
        @DisplayName("[T7-27] Redis 장애로 서비스가 TEMPORARILY_FAILED → 503 RANKING_TEMPORARILY_FAILED")
        void unavailable() throws Exception {
            // Given
            given(queryService.weeklyUsers(isNull(), anyInt()))
                    .willThrow(new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED));

            // When
            ResultActions result = mockMvc.perform(get(WEEKLY));

            // Then
            expectError(result, RankingErrorCode.TEMPORARILY_FAILED);
        }
    }

    @Nested
    @DisplayName("GET /api/rankings/users/all — 누적 유저")
    class All {

        @Test
        @DisplayName("[T7-19] size 를 생략하면 10 으로 조회하고, 200 과 {week:null, items[...]}")
        void defaultSize() throws Exception {
            // Given
            given(queryService.allUsers(10)).willReturn(USERS);

            // When
            ResultActions result = mockMvc.perform(get(ALL));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("{\"week\":null,\"items\":" + USERS_JSON + "}", JsonCompareMode.STRICT));
            verify(queryService).allUsers(10);
        }

        @Test
        @DisplayName("[T7-19] ?size=0 → 서비스가 INVALID_SIZE → 400 RANKING_INVALID_SIZE")
        void invalidSize() throws Exception {
            // Given
            given(queryService.allUsers(0)).willThrow(new ApplicationException(RankingErrorCode.INVALID_SIZE));

            // When
            ResultActions result = mockMvc.perform(get(ALL).param("size", "0"));

            // Then
            expectError(result, RankingErrorCode.INVALID_SIZE);
        }

        @Test
        @DisplayName("[T7-27] Redis 장애로 서비스가 TEMPORARILY_FAILED → 503 RANKING_TEMPORARILY_FAILED")
        void unavailable() throws Exception {
            // Given
            given(queryService.allUsers(anyInt())).willThrow(new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED));

            // When
            ResultActions result = mockMvc.perform(get(ALL));

            // Then
            expectError(result, RankingErrorCode.TEMPORARILY_FAILED);
        }
    }

    @Nested
    @DisplayName("GET /api/rankings/users/me — 내 순위")
    class Me {

        @Test
        @DisplayName("[T7-19] X-User-Id 로 조회하고 200 과 {week, weeklyRank, weeklyScore, allRank, allScore}")
        void ranked() throws Exception {
            // Given
            given(queryService.myRank(7L)).willReturn(new MyRankResult("2026-W47", 12L, 3, 40L, 9));

            // When
            ResultActions result = mockMvc.perform(get(ME).header("X-User-Id", "7"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"week":"2026-W47","weeklyRank":12,"weeklyScore":3,"allRank":40,"allScore":9}""",
                            JsonCompareMode.STRICT));
            verify(queryService).myRank(7L);
        }

        @Test
        @DisplayName("[T7-19] 랭킹에 없으면 weeklyRank·allRank 는 null, 점수는 0 으로 응답한다")
        void notRanked() throws Exception {
            // Given
            given(queryService.myRank(7L)).willReturn(new MyRankResult("2026-W47", null, 0, null, 0));

            // When
            ResultActions result = mockMvc.perform(get(ME).header("X-User-Id", "7"));

            // Then
            result.andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"week":"2026-W47","weeklyRank":null,"weeklyScore":0,"allRank":null,"allScore":0}""",
                            JsonCompareMode.STRICT));
        }

        @Test
        @DisplayName("[T7-19] X-User-Id 가 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
        void missingUserHeader() throws Exception {
            // When
            ResultActions result = mockMvc.perform(get(ME));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
            verifyNoInteractions(queryService);
        }

        @ParameterizedTest(name = "X-User-Id={0}")
        @ValueSource(strings = {"abc", "1.5"})
        @DisplayName("[T7-19 보강] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
        void invalidUserHeader(String userId) throws Exception {
            // When
            ResultActions result = mockMvc.perform(get(ME).header("X-User-Id", userId));

            // Then
            result.andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
            verifyNoInteractions(queryService);
        }

        @Test
        @DisplayName("[T7-27] Redis 장애로 서비스가 TEMPORARILY_FAILED → 503 RANKING_TEMPORARILY_FAILED")
        void unavailable() throws Exception {
            // Given
            given(queryService.myRank(anyLong())).willThrow(new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED));

            // When
            ResultActions result = mockMvc.perform(get(ME).header("X-User-Id", "7"));

            // Then
            expectError(result, RankingErrorCode.TEMPORARILY_FAILED);
        }
    }
}
