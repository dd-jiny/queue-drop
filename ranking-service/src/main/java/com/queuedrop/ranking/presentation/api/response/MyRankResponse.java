package com.queuedrop.ranking.presentation.api.response;

import com.queuedrop.ranking.application.dto.MyRankResult;

/** weeklyRank/allRank 는 랭킹에 없으면 null */
public record MyRankResponse(String week, Long weeklyRank, long weeklyScore, Long allRank, long allScore) {

    public static MyRankResponse from(MyRankResult r) {
        return new MyRankResponse(r.week(), r.weeklyRank(), r.weeklyScore(), r.allRank(), r.allScore());
    }
}
