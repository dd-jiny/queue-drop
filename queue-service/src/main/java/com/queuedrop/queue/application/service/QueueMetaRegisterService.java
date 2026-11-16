package com.queuedrop.queue.application.service;

import com.queuedrop.queue.application.dto.QueueMetaRegisterCommand;
import com.queuedrop.queue.domain.exception.DomainException;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** drop.created 처리. queue 는 저장 컨슈머가 아니므로 DLT 없이 잘못된 메시지는 로그 후 스킵한다 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueueMetaRegisterService {

    private final WaitingQueueRepository queueRepository;

    public void register(QueueMetaRegisterCommand cmd) {
        QueueMeta meta;
        try {
            meta = new QueueMeta(cmd.dropId(), cmd.openAt(), cmd.closeAt());
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", cmd.dropId(), e.getMessage());
            return;
        }
        queueRepository.saveMeta(meta);
    }
}
