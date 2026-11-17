package com.queuedrop.ranking.domain.repository;

import com.queuedrop.ranking.domain.model.DropWindow;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

public interface DropWindowRepository {

    void save(long dropId, Instant closeAt);

    /** 없는 id 는 closeAt=null 로 채움 */
    Map<Long, DropWindow> findAll(Collection<Long> dropIds);
}
