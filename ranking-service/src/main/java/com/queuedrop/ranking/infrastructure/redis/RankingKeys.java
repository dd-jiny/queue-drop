package com.queuedrop.ranking.infrastructure.redis;

import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import java.util.Objects;

/** Redis 키 이름 매핑 (phase-7-ranking.md 3.0) */
final class RankingKeys {
    static final String DROP_POPULAR  = "rank:drop:popular";
    static final String USER_ALL      = "rank:user:all";
    static final String DROP_INFO     = "rank:drop:info";
    static final String USER_INFO     = "rank:user:info";
    static final String DROP_CLOSE_AT = "rank:drop:closeAt";   // (구현 결정)

    private RankingKeys() {}

    static String weekly(WeekKey week) {
        return "rank:user:weekly:" + week.value();
    }

    static String dedupe(long dropId) {
        return "rank:dedupe:" + dropId;
    }

    static String board(RankingBoard board, WeekKey week) {
        return switch (board) {
            case DROP_POPULAR -> DROP_POPULAR;
            case USER_ALL     -> USER_ALL;
            case USER_WEEKLY  -> weekly(Objects.requireNonNull(week, "USER_WEEKLY 는 week 필수"));
        };
    }
}
