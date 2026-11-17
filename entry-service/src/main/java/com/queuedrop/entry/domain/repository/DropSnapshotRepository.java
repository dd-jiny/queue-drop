package com.queuedrop.entry.domain.repository;

import com.queuedrop.entry.domain.model.DropSnapshot;
import java.util.Optional;

/** 드로우 스냅샷 영속성 포트. */
public interface DropSnapshotRepository {

    /** 이미 있으면 저장하지 않고 false. drop.created 재수신 대비 멱등 저장. */
    boolean saveIfAbsent(DropSnapshot snapshot);

    Optional<DropSnapshot> findById(long dropId);
}
