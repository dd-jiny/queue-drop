package com.queuedrop.entry.application.service;

import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * entry.created 처리. @Transactional 을 걸지 않는다 — saveIfAbsent 가 자체 트랜잭션을 가지며,
 * 바깥 트랜잭션이 있으면 제약 위반 시 rollback-only 로 UnexpectedRollbackException 이 난다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntrySaveService {

    private final EntryRepository entryRepository;
    private final EntryGate entryGate;

    public void save(EntrySaveCommand command) {
        // ① 보상된 응모(유령 이벤트) 거르기: Redis 응모자에 없으면 저장하지 않는다
        if (!entryGate.isApplicant(command.dropId(), command.userId())) {
            log.warn("보상된 응모 이벤트 무시 dropId={} userId={} seq={}",
                    command.dropId(), command.userId(), command.entrySeq());
            return;
        }

        // ② 같은 사용자 응모가 이미 있으면 무시 (재전달, 또는 유령 + 재응모)
        Entry entry = Entry.apply(command.dropId(), command.userId(),
                command.entrySeq(), command.appliedAt());
        if (!entryRepository.saveIfAbsent(entry)) {
            log.debug("이미 저장된 응모 dropId={} userId={}", command.dropId(), command.userId());
        }
    }
}
