package com.queuedrop.entry.presentation.listener;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.entry.application.dto.DropSnapshotRegisterCommand;
import com.queuedrop.entry.application.service.DropSnapshotRegisterService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 예외를 잡지 않는다: 잘못된 값은 application 이 스킵하고, DB·Redis 장애만 올라와 재시도 → drop.created.DLT.
 * idIsGroup = false 로 group-id 는 공통 설정(entry-service) 그대로 둔다.
 */
@Component
@RequiredArgsConstructor
public class DropCreatedListener {

    private final DropSnapshotRegisterService registerService;

    @KafkaListener(id = "entry-drop-created", idIsGroup = false, topics = Topics.DROP_CREATED)
    public void on(DropCreated message) {
        registerService.register(new DropSnapshotRegisterCommand(
                message.dropId(), message.entryCapacity(), message.winnerCount(),
                message.openAt(), message.closeAt()));
    }
}
