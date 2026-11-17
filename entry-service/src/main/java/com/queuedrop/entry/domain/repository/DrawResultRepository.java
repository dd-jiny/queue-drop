package com.queuedrop.entry.domain.repository;

import com.queuedrop.entry.domain.model.DrawResult;
import java.util.List;
import java.util.Optional;

/** 추첨 기록 영속성 포트. */
public interface DrawResultRepository {

    DrawResult save(DrawResult result);

    Optional<DrawResult> findByDropId(long dropId);

    List<DrawResult> findUnpublished();

    void markPublished(long dropId);
}
