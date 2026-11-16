package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * (dropId, userId) 키 인메모리 저장소. 키가 있으면 false (ON CONFLICT DO NOTHING 과 같은 의미).
 * 같은 드로우에서 entrySeq 가 겹치면 예외 (실제 DB 유니크 제약과 같은 의미).
 */
public class InMemoryEntryRepository implements EntryRepository {

    private record Key(long dropId, long userId) {}

    private final Map<Key, Entry> store = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();

    @Override
    public boolean saveIfAbsent(Entry entry) {
        Key key = new Key(entry.getDropId(), entry.getUserId());
        if (store.containsKey(key)) {
            return false;
        }
        boolean seqConflict = store.values().stream()
                .anyMatch(e -> e.getDropId() == entry.getDropId() && e.getEntrySeq() == entry.getEntrySeq());
        if (seqConflict) {
            throw new IllegalStateException("entrySeq 충돌: dropId=" + entry.getDropId()
                    + ", entrySeq=" + entry.getEntrySeq());
        }
        store.put(key, Entry.restore(idSequence.incrementAndGet(), entry.getDropId(), entry.getUserId(),
                entry.getEntrySeq(), entry.getStatus(), entry.getAppliedAt()));
        return true;
    }

    @Override
    public Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return Optional.ofNullable(store.get(new Key(dropId, userId)));
    }

    /** 최신순 (appliedAt 내림차순, 같으면 id 내림차순). */
    @Override
    public List<Entry> findAllByUserId(long userId) {
        return store.values().stream()
                .filter(e -> e.getUserId() == userId)
                .sorted(Comparator.comparing(Entry::getAppliedAt).thenComparing(Entry::getId).reversed())
                .toList();
    }

    public int count() {
        return store.size();
    }
}
