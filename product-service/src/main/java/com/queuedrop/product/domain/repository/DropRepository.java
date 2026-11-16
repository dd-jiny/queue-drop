package com.queuedrop.product.domain.repository;

import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;

import java.time.Instant;
import java.util.Optional;

public interface DropRepository {
    Drop save(Drop drop);

    Optional<Drop> findById(Long id);

    /** status 가 null 이면 전체. 상태는 Drop.status(now) 와 같은 경계의 시각 조건으로 변환해 조회한다. */
    DropPage search(DropStatus status, Instant now, int page, int size);
}
