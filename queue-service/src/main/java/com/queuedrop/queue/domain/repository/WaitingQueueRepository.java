package com.queuedrop.queue.domain.repository;

import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** 대기열 저장소 포트 (class-diagram 2.3). 구현은 infrastructure/redis. */
public interface WaitingQueueRepository {

    /** 멱등. 같은 드로우 재수신 시 같은 값으로 덮어쓴다. 대기열(waiting·seq)은 건드리지 않는다 */
    void saveMeta(QueueMeta meta);

    Optional<QueueMeta> findMeta(Long dropId);

    /** 원자적: 토큰 보유 → alreadyAdmitted / 대기 중 → 현재 rank / 신규 → 순번 발급 후 rank */
    EnqueueResult enqueue(Long dropId, Long userId);

    boolean hasToken(Long dropId, Long userId);

    /** 대기 중이 아니면 empty */
    OptionalLong rank(Long dropId, Long userId);

    /** 앞에서 최대 n 명을 꺼내 토큰 발급. 잔여 정원이 없거나(키 없음 포함) 0 이하이면 아무도 입장시키지 않는다 */
    List<Long> admit(Long dropId, int n, Duration tokenTtl, Instant now);

    /** closeAt > now 인 드로우 ID (openAt 판단은 호출 측) */
    List<Long> findActiveDropIds(Instant now);

    void removeExpiredDrops(Instant now);
}
