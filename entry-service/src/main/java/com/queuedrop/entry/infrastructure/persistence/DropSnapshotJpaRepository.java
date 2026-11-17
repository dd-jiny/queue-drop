package com.queuedrop.entry.infrastructure.persistence;

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
}
