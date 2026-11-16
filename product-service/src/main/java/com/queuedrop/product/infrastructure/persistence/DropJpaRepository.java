package com.queuedrop.product.infrastructure.persistence;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

/**
 * 상태 → 시각 조건 변환. Drop.status(now) 와 같은 경계([openAt, closeAt), drawn 우선)를 쓴다.
 * 규칙을 바꾸면 Drop.status 와 함께 수정한다 (T2-19 가 일치를 고정).
 */
interface DropJpaRepository extends JpaRepository<DropJpaEntity, Long> {

    @Query("select d from DropJpaEntity d where d.drawn = false and d.openAt > :now")
    Page<DropJpaEntity> findScheduled(@Param("now") Instant now, Pageable pageable);

    @Query("select d from DropJpaEntity d where d.drawn = false and d.openAt <= :now and d.closeAt > :now")
    Page<DropJpaEntity> findOpen(@Param("now") Instant now, Pageable pageable);

    @Query("select d from DropJpaEntity d where d.drawn = false and d.closeAt <= :now")
    Page<DropJpaEntity> findClosed(@Param("now") Instant now, Pageable pageable);

    Page<DropJpaEntity> findByDrawnTrue(Pageable pageable);
}
