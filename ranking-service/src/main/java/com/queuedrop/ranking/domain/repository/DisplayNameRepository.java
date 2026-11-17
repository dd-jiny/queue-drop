package com.queuedrop.ranking.domain.repository;

import java.util.Collection;
import java.util.Map;

public interface DisplayNameRepository {

    void saveDropTitle(long dropId, String title);

    void saveNickname(long userId, String nickname);

    /** 없는 id 는 맵에 없음 */
    Map<Long, String> findDropTitles(Collection<Long> dropIds);

    /** 없는 id 는 맵에 없음 */
    Map<Long, String> findNicknames(Collection<Long> userIds);
}
