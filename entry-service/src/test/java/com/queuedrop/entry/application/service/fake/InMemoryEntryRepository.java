package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import com.queuedrop.entry.domain.repository.EntryRepository;
import com.queuedrop.entry.domain.repository.EntrySaveOutcome;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * (dropId, userId) 키 인메모리 저장소. 키가 있으면 ALREADY_EXISTS (ON CONFLICT DO NOTHING 과 같은 의미).
 * 연동한 InMemoryDropSnapshotRepository 에서 드로우가 DRAWN 이면 LOST 로 저장하고 SAVED_AS_LOST (E-48).
 * 스냅샷이 없거나 연동하지 않았으면 APPLIED 로 저장하고 SAVED.
 * 같은 드로우에서 entrySeq 가 겹치면 예외 (실제 DB 유니크 제약과 같은 의미).
 * markResults 는 APPLIED 행만 바꾼다 (DB 벌크 UPDATE 의 status='APPLIED' 조건과 같은 의미).
 */
public class InMemoryEntryRepository implements EntryRepository {

    private record Key(long dropId, long userId) {}

    private final Map<Key, Entry> store = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();
    private RuntimeException markResultsFailure;
    private final Map<Long, RuntimeException> countFailures = new HashMap<>();
    private final InMemoryDropSnapshotRepository snapshots;

    /** 스냅샷 연동 없음 — 항상 APPLIED 로 저장. */
    public InMemoryEntryRepository() {
        this(null);
    }

    /** 스냅샷 상태가 DRAWN 이면 LOST 로 저장한다 (EntryRepositoryImpl 의 FOR SHARE 판정과 같은 의미). */
    public InMemoryEntryRepository(InMemoryDropSnapshotRepository snapshots) {
        this.snapshots = snapshots;
    }

    /** 다음 countByDropId(dropId) 한 번만 이 예외를 던진다. */
    public synchronized void failCountOnceFor(long dropId, RuntimeException failure) {
        countFailures.put(dropId, failure);
    }

    /** 이후 markResults 호출이 이 예외를 던진다 (아무 행도 바꾸지 않음). */
    public void failMarkResultsWith(RuntimeException failure) {
        this.markResultsFailure = failure;
    }

    @Override
    public synchronized EntrySaveOutcome saveIfAbsent(Entry entry) {
        Key key = new Key(entry.getDropId(), entry.getUserId());
        if (store.containsKey(key)) {
            return EntrySaveOutcome.ALREADY_EXISTS;
        }
        boolean seqConflict = store.values().stream()
                .anyMatch(e -> e.getDropId() == entry.getDropId() && e.getEntrySeq() == entry.getEntrySeq());
        if (seqConflict) {
            throw new IllegalStateException("entrySeq 충돌: dropId=" + entry.getDropId()
                    + ", entrySeq=" + entry.getEntrySeq());
        }
        boolean drawn = snapshots != null && snapshots.findById(entry.getDropId())
                .map(s -> s.getStatus() == SnapshotStatus.DRAWN).orElse(false);
        EntryStatus status = drawn ? EntryStatus.LOST : entry.getStatus();
        store.put(key, Entry.restore(idSequence.incrementAndGet(), entry.getDropId(), entry.getUserId(),
                entry.getEntrySeq(), status, entry.getAppliedAt()));
        return drawn ? EntrySaveOutcome.SAVED_AS_LOST : EntrySaveOutcome.SAVED;
    }

    @Override
    public synchronized Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return Optional.ofNullable(store.get(new Key(dropId, userId)));
    }

    /** 최신순 (appliedAt 내림차순, 같으면 id 내림차순). */
    @Override
    public synchronized List<Entry> findAllByUserId(long userId) {
        return store.values().stream()
                .filter(e -> e.getUserId() == userId)
                .sorted(Comparator.comparing(Entry::getAppliedAt).thenComparing(Entry::getId).reversed())
                .toList();
    }

    @Override
    public synchronized long countByDropId(long dropId) {
        RuntimeException failure = countFailures.remove(dropId);
        if (failure != null) {
            throw failure;
        }
        return store.values().stream().filter(e -> e.getDropId() == dropId).count();
    }

    /** entrySeq 오름차순 userId. 상태와 무관하게 전원. */
    @Override
    public synchronized List<Long> findUserIdsOrderBySeq(long dropId) {
        return store.values().stream()
                .filter(e -> e.getDropId() == dropId)
                .sorted(Comparator.comparingInt(Entry::getEntrySeq))
                .map(Entry::getUserId)
                .toList();
    }

    @Override
    public synchronized void markResults(long dropId, Set<Long> winnerUserIds) {
        if (markResultsFailure != null) {
            throw markResultsFailure;
        }
        store.replaceAll((key, e) -> {
            if (e.getDropId() != dropId || e.getStatus() != EntryStatus.APPLIED) {
                return e;
            }
            EntryStatus next = winnerUserIds.contains(e.getUserId()) ? EntryStatus.WON : EntryStatus.LOST;
            return Entry.restore(e.getId(), e.getDropId(), e.getUserId(), e.getEntrySeq(), next, e.getAppliedAt());
        });
    }

    public synchronized int count() {
        return store.size();
    }
}
