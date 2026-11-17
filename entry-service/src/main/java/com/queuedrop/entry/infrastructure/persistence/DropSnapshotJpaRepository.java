package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.SnapshotStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface DropSnapshotJpaRepository extends JpaRepository<DropSnapshotJpaEntity, Long> {

    /**
     * save() 는 직접 지정한 @Id 를 merge 로 처리해 기존 행을 덮어쓰므로, 없을 때만 넣는 native INSERT 를 쓴다.
     * @return 1 = 저장, 0 = 이미 있음
     */
    @Modifying
    @Query(value = """
            INSERT INTO drop_snapshots (drop_id, entry_capacity, winner_count, open_at, close_at, status)
            VALUES (:#{#e.dropId}, :#{#e.entryCapacity}, :#{#e.winnerCount},
                    :#{#e.openAt}, :#{#e.closeAt}, :#{#e.status.name()})
            ON CONFLICT (drop_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("e") DropSnapshotJpaEntity entity);

    @Query("select s.dropId from DropSnapshotJpaEntity s"
            + " where s.status = com.queuedrop.entry.domain.model.SnapshotStatus.OPEN and s.closeAt <= :now")
    List<Long> findDueOpenIds(@Param("now") Instant now);

    /** @return 1 = 이 호출이 전이시킴, 0 = 조건 불일치(이미 전이됨·마감 전·없음) */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE drop_snapshots SET status = 'CLOSING'
             WHERE drop_id = :dropId AND status = 'OPEN' AND close_at <= :now
            """, nativeQuery = true)
    int markClosingIfOpen(@Param("dropId") long dropId, @Param("now") Instant now);

    /** @return 1 = 이 호출이 전이시킴, 0 = CLOSING 아님 */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE drop_snapshots SET status = 'DRAWN'
             WHERE drop_id = :dropId AND status = 'CLOSING'
            """, nativeQuery = true)
    int markDrawnIfClosing(@Param("dropId") long dropId);

    List<DropSnapshotJpaEntity> findByStatus(SnapshotStatus status);

    /** 응모 저장용 상태 조회. FOR SHARE 로 추첨의 CLOSING → DRAWN UPDATE 와 직렬화한다 (E-48). */
    @Query(value = "SELECT status FROM drop_snapshots WHERE drop_id = :dropId FOR SHARE", nativeQuery = true)
    Optional<String> findStatusForShare(@Param("dropId") long dropId);
}
