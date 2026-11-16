package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** dropId 키 인메모리 저장소. 이미 있으면 false, 첫 값 유지. */
public class InMemoryDropSnapshotRepository implements DropSnapshotRepository {

    private final Map<Long, DropSnapshot> store = new LinkedHashMap<>();

    @Override
    public boolean saveIfAbsent(DropSnapshot snapshot) {
        return store.putIfAbsent(snapshot.getDropId(), snapshot) == null;
    }

    @Override
    public Optional<DropSnapshot> findById(long dropId) {
        return Optional.ofNullable(store.get(dropId));
    }

    public int count() {
        return store.size();
    }
}
