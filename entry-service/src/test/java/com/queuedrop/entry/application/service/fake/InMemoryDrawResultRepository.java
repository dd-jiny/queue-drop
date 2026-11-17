package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * dropId 키 인메모리 저장소. save 는 AtomicLong 으로 id 를 부여한 인스턴스를 돌려준다 (DB IDENTITY 와 같은 의미).
 * 같은 dropId 재저장은 DataIntegrityViolationException (DrawResultRepositoryImpl.saveAndFlush 의 uk_draw_results_drop_id 위반과 같은 예외).
 */
public class InMemoryDrawResultRepository implements DrawResultRepository {

    private final Map<Long, DrawResult> store = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();
    private RuntimeException markPublishedFailure;

    /** 다음 markPublished 한 번만 이 예외를 던진다 (published 는 바뀌지 않음). */
    public synchronized void failMarkPublishedNext(RuntimeException failure) {
        this.markPublishedFailure = failure;
    }

    @Override
    public synchronized DrawResult save(DrawResult result) {
        if (store.containsKey(result.getDropId())) {
            throw new DataIntegrityViolationException(
                    "duplicate key value violates unique constraint \"uk_draw_results_drop_id\": dropId="
                            + result.getDropId());
        }
        DrawResult saved = DrawResult.restore(idSequence.incrementAndGet(), result.getDropId(), result.getSeed(),
                result.getApplicantCount(), result.getWinnerCount(), result.getDrawnAt(), result.isPublished());
        store.put(saved.getDropId(), saved);
        return saved;
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
        if (markPublishedFailure != null) {
            RuntimeException failure = markPublishedFailure;
            markPublishedFailure = null;
            throw failure;
        }
        store.computeIfPresent(dropId, (id, r) -> r.markPublished());
    }

    public synchronized int count() {
        return store.size();
    }
}
