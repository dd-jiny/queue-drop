package com.queuedrop.ranking.domain.model;

/** 랭킹 보드 종류. 키 문자열은 infrastructure/redis/RankingKeys 가 매핑한다. */
public enum RankingBoard {
    DROP_POPULAR,   // 인기 드로우 (member = dropId)
    USER_WEEKLY,    // 주간 유저 (member = userId, WeekKey 필요)
    USER_ALL        // 누적 유저 (member = userId)
}
