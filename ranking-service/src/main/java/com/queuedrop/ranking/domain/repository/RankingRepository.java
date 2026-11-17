package com.queuedrop.ranking.domain.repository;

import com.queuedrop.ranking.domain.model.RankEntry;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface RankingRepository {

    /**
     * (dropId, userId) 를 처음 보면 3개 보드에 1점씩 더하고 true, 이미 집계했으면 false.
     * dedupe 와 증가는 원자적으로 수행되어야 한다.
     */
    boolean incrementOnce(long dropId, long userId, WeekKey week);

    /** 점수 내림차순 [offset, offset+count). week 는 USER_WEEKLY 일 때만 필수. */
    List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count);

    /** 해당 보드에 없으면 empty. */
    Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId);

    /** 인기 보드에서 종료 드로우 제거 (ZREM). 없는 id 는 무시 — 멱등 */
    void removeDrops(Collection<Long> dropIds);
}
