package com.queuedrop.entry.infrastructure.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface EntryJpaRepository extends JpaRepository<EntryJpaEntity, Long> {

    /**
     * (drop_id, user_id) 충돌만 무시한다. entry_seq 충돌은 예외로 올린다.
     * @return 1 = 저장, 0 = 같은 사용자 응모가 이미 있음
     */
    @Modifying
    @Query(value = """
            INSERT INTO entries (drop_id, user_id, entry_seq, status, applied_at, updated_at)
            VALUES (:dropId, :userId, :entrySeq, :status, :appliedAt, now())
            ON CONFLICT (drop_id, user_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("dropId") long dropId, @Param("userId") long userId,
                       @Param("entrySeq") int entrySeq, @Param("status") String status,
                       @Param("appliedAt") Instant appliedAt);

    Optional<EntryJpaEntity> findByDropIdAndUserId(long dropId, long userId);

    List<EntryJpaEntity> findAllByUserIdOrderByAppliedAtDesc(long userId);

    long countByDropId(long dropId);

    @Query(value = "SELECT user_id FROM entries WHERE drop_id = :dropId ORDER BY entry_seq", nativeQuery = true)
    List<Long> findUserIdsOrderBySeq(@Param("dropId") long dropId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE entries SET status = 'WON', updated_at = now()
             WHERE drop_id = :dropId AND status = 'APPLIED' AND user_id IN (:userIds)
            """, nativeQuery = true)
    int markWon(@Param("dropId") long dropId, @Param("userIds") Collection<Long> userIds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE entries SET status = 'LOST', updated_at = now()
             WHERE drop_id = :dropId AND status = 'APPLIED'
            """, nativeQuery = true)
    int markRemainingLost(@Param("dropId") long dropId);
}
