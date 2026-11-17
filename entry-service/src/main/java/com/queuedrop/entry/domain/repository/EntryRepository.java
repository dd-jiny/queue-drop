package com.queuedrop.entry.domain.repository;

import com.queuedrop.entry.domain.model.Entry;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** 응모 영속성 포트. */
public interface EntryRepository {

    /**
     * (dropId, userId) 응모가 없을 때만 저장한다.
     *
     * @return 저장했으면 true, 같은 사용자 응모가 이미 있으면 false
     * @throws RuntimeException 응모 번호(entrySeq) 충돌 등 그 밖의 제약 위반 (정상이라면 일어나지 않음)
     */
    boolean saveIfAbsent(Entry entry);

    Optional<Entry> findByDropIdAndUserId(long dropId, long userId);

    List<Entry> findAllByUserId(long userId);

    long countByDropId(long dropId);

    /** entry_seq 오름차순 userId. 상태와 무관하게 전원 (추첨 입력의 결정적 순서). */
    List<Long> findUserIdsOrderBySeq(long dropId);

    /** winners → WON, 나머지 APPLIED → LOST (벌크). 이미 확정된 행은 바꾸지 않는다. */
    void markResults(long dropId, Set<Long> winnerUserIds);
}
