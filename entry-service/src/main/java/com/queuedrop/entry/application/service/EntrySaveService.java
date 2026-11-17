package com.queuedrop.entry.application.service;

import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
import com.queuedrop.entry.domain.repository.EntrySaveOutcome;
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
        // ① 저장 확정: 보상된 응모 또는 이전 시도의 유령 이벤트면 false. true 이후 보상은 거절된다 (E-40)
        if (!entryGate.claimForSave(command.dropId(), command.userId(), command.eventId())) {
            log.warn("유령 응모 이벤트 무시 dropId={} userId={} seq={} eventId={}",
                    command.dropId(), command.userId(), command.entrySeq(), command.eventId());
            return;
        }

        // ② 같은 사용자 응모가 이미 있으면 무시 (재전달, 또는 유령 + 재응모)
        Entry entry = Entry.apply(command.dropId(), command.userId(),
                command.entrySeq(), command.appliedAt());
        EntrySaveOutcome outcome = entryRepository.saveIfAbsent(entry);
        if (outcome == EntrySaveOutcome.SAVED_AS_LOST) {
            // ③ 수동 추첨 뒤 늦게 도착한 응모: 낙첨으로 저장됨 (E-48, 운영 확인용)
            log.warn("late entry saved as LOST after draw dropId={} userId={} eventId={}",
                    command.dropId(), command.userId(), command.eventId());
        } else if (outcome == EntrySaveOutcome.ALREADY_EXISTS) {
            log.debug("이미 저장된 응모 dropId={} userId={}", command.dropId(), command.userId());
        }
    }
}
