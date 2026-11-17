package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * dropId 키 인메모리 저장소. 이미 있으면 false, 첫 값 유지.
 * 상태 전이는 DB 조건부 UPDATE 와 같은 의미 (조건이 맞을 때만 전이하고 true).
 */
public class InMemoryDropSnapshotRepository implements DropSnapshotRepository {

    private final Map<Long, DropSnapshot> store = new LinkedHashMap<>();
    private RuntimeException saveFailure;
    private final Map<Long, RuntimeException> markClosingFailures = new HashMap<>();

    /** 다음 markClosingIfOpen(dropId) 한 번만 이 예외를 던진다 (상태는 바꾸지 않음). */
    public synchronized void failMarkClosingOnceFor(long dropId, RuntimeException failure) {
        markClosingFailures.put(dropId, failure);
    }

    /** 이후 saveIfAbsent 호출이 이 예외를 던진다. */
    public void failSaveWith(RuntimeException failure) {
        this.saveFailure = failure;
    }

    @Override
    public synchronized boolean saveIfAbsent(DropSnapshot snapshot) {
        if (saveFailure != null) {
            throw saveFailure;
        }
        return store.putIfAbsent(snapshot.getDropId(), snapshot) == null;
    }

    @Override
    public synchronized Optional<DropSnapshot> findById(long dropId) {
        return Optional.ofNullable(store.get(dropId));
    }

    @Override
    public synchronized List<Long> findDueOpenIds(Instant now) {
        return store.values().stream()
                .filter(s -> s.getStatus() == SnapshotStatus.OPEN && !s.getCloseAt().isAfter(now))
                .map(DropSnapshot::getDropId)
                .toList();
    }

    @Override
    public synchronized boolean markClosingIfOpen(long dropId, Instant now) {
        RuntimeException failure = markClosingFailures.remove(dropId);
        if (failure != null) {
            throw failure;
        }
        DropSnapshot s = store.get(dropId);
        if (s == null || s.getStatus() != SnapshotStatus.OPEN || s.getCloseAt().isAfter(now)) {
            return false;
        }
        store.put(dropId, withStatus(s, SnapshotStatus.CLOSING));
        return true;
    }

    @Override
    public synchronized List<DropSnapshot> findClosing() {
        return store.values().stream().filter(s -> s.getStatus() == SnapshotStatus.CLOSING).toList();
    }

    @Override
    public synchronized boolean markDrawnIfClosing(long dropId) {
        DropSnapshot s = store.get(dropId);
        if (s == null || s.getStatus() != SnapshotStatus.CLOSING) {
            return false;
        }
        store.put(dropId, withStatus(s, SnapshotStatus.DRAWN));
        return true;
    }

    /** 테스트 준비용: 상태를 직접 바꾼다 (예: CLOSING 으로 둔 드로우). */
    public synchronized void forceStatus(long dropId, SnapshotStatus status) {
        store.computeIfPresent(dropId, (id, s) -> withStatus(s, status));
    }

    public synchronized int count() {
        return store.size();
    }

    private static DropSnapshot withStatus(DropSnapshot s, SnapshotStatus status) {
        return DropSnapshot.restore(s.getDropId(), s.getEntryCapacity(), s.getWinnerCount(),
                s.getOpenAt(), s.getCloseAt(), status);
    }
}
