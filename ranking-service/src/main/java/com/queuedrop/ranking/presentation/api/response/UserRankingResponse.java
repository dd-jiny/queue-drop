package com.queuedrop.ranking.presentation.api.response;

import com.queuedrop.ranking.application.dto.UserRankResult;
import com.queuedrop.ranking.application.dto.WeeklyRankingResult;
import java.util.List;

/** 주간은 week 가 조회한 주차, 누적은 week = null */
public record UserRankingResponse(String week, List<Item> items) {

    public record Item(long rank, long userId, String nickname, long score) {

        static Item from(UserRankResult r) {
            return new Item(r.rank(), r.userId(), r.nickname(), r.score());
        }
    }

    public static UserRankingResponse from(WeeklyRankingResult r) {
        return new UserRankingResponse(r.week(), items(r.items()));
    }

    public static UserRankingResponse ofAll(List<UserRankResult> r) {
        return new UserRankingResponse(null, items(r));
    }

    private static List<Item> items(List<UserRankResult> results) {
        return results.stream().map(Item::from).toList();
    }
}
