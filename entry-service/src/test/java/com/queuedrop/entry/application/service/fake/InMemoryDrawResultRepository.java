package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** dropId 키 인메모리 저장소. 같은 dropId 재저장은 예외 (DB uk_draw_results_drop_id 와 같은 의미). */
public class InMemoryDrawResultRepository implements DrawResultRepository {

    private final Map<Long, DrawResult> store = new LinkedHashMap<>();

    @Override
    public synchronized DrawResult save(DrawResult result) {
        if (store.containsKey(result.getDropId())) {
            throw new IllegalStateException("draw_results drop_id 중복: dropId=" + result.getDropId());
        }
        store.put(result.getDropId(), result);
        return result;
    }

    @Override
    public synchronized Optional<DrawResult> findByDropId(long dropId) {
        return Optional.ofNullable(store.get(dropId));
    }

    @Override
    public synchronized List<DrawResult> findUnpublished() {
        return store.values().stream().filter(r -> !r.isPublished()).toList();
    }

    @Override
    public synchronized void markPublished(long dropId) {
        store.computeIfPresent(dropId, (id, r) -> r.markPublished());
    }

    public synchronized int count() {
        return store.size();
    }
}
