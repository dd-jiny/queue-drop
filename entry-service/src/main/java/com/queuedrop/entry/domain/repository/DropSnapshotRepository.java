package com.queuedrop.entry.domain.repository;

import com.queuedrop.entry.domain.model.DropSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 드로우 스냅샷 영속성 포트. */
public interface DropSnapshotRepository {

    /** 이미 있으면 저장하지 않고 false. drop.created 재수신 대비 멱등 저장. */
    boolean saveIfAbsent(DropSnapshot snapshot);

    Optional<DropSnapshot> findById(long dropId);

    /** status=OPEN 이고 closeAt <= now 인 드로우 id 목록 */
    List<Long> findDueOpenIds(Instant now);

    /** OPEN → CLOSING 조건부 전이. 이 호출이 전이시켰으면 true (다중 인스턴스 중 1곳만 true) */
    boolean markClosingIfOpen(long dropId, Instant now);

    List<DropSnapshot> findClosing();

    /** CLOSING → DRAWN 조건부 전이. 추첨 트랜잭션의 첫 문장 (행 잠금 획득) */
    boolean markDrawnIfClosing(long dropId);
}
