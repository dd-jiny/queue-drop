package com.queuedrop.entry.infrastructure.persistence;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface DrawResultJpaRepository extends JpaRepository<DrawResultJpaEntity, Long> {

    Optional<DrawResultJpaEntity> findByDropId(long dropId);

    List<DrawResultJpaEntity> findByPublishedFalse();

    /** 이미 true 여도 그대로 true (재발행 멱등). */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE draw_results SET published = TRUE WHERE drop_id = :dropId", nativeQuery = true)
    int markPublished(@Param("dropId") long dropId);
}
