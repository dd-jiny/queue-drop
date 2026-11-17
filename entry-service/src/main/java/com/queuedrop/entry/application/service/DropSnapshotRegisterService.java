package com.queuedrop.entry.application.service;

import com.queuedrop.entry.application.dto.DropSnapshotRegisterCommand;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.domain.exception.DomainException;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * drop.created 처리. DB 저장 후 Redis 초기화 — 둘 다 멱등이라 어느 단계에서 실패해도 재전달로 복구된다.
 * 잘못된 메시지는 로그 후 스킵한다 (queue 의 QueueMetaRegisterService 와 같은 방식).
 * @Transactional 을 걸지 않는다 (커밋 실패 시 Redis 만 초기화된 상태가 남지 않도록).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DropSnapshotRegisterService {

    private final DropSnapshotRepository dropSnapshotRepository;
    private final EntryGate entryGate;

    public void register(DropSnapshotRegisterCommand command) {
        DropSnapshot snapshot;
        try {
            snapshot = DropSnapshot.register(
                    command.dropId(), command.entryCapacity(), command.winnerCount(),
                    command.openAt(), command.closeAt());
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", command.dropId(), e.getMessage());
            return;
        }

        // ① DB (자체 트랜잭션). 이미 있으면 재전달 — 메시지 값이 아니라 저장된 첫 값을 기준으로 한다
        DropSnapshot stored = dropSnapshotRepository.saveIfAbsent(snapshot)
                ? snapshot
                : dropSnapshotRepository.findById(snapshot.getDropId()).orElseThrow();

        entryGate.initialize(stored.getDropId(), stored.getEntryCapacity(),   // ② Redis (NX)
                stored.getOpenAt(), stored.getCloseAt());
    }
}
