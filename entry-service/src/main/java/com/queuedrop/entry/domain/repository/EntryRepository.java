package com.queuedrop.entry.domain.repository;

import com.queuedrop.entry.domain.model.Entry;
import java.util.List;
import java.util.Optional;

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
}
