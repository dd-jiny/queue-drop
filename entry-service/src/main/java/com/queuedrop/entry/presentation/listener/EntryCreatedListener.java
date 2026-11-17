package com.queuedrop.entry.presentation.listener;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.service.EntrySaveService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 예외를 잡지 않는다: DB 장애 등은 DefaultErrorHandler 가 재시도 → entry.created.DLT.
 * id 는 유령 이벤트 테스트에서 컨테이너를 pause 하는 데 쓴다. group-id 는 entry-service 그대로 (idIsGroup = false).
 */
@Component
@RequiredArgsConstructor
public class EntryCreatedListener {

    private final EntrySaveService entrySaveService;

    @KafkaListener(id = "entry-saver", idIsGroup = false, topics = Topics.ENTRY_CREATED)
    public void on(EntryCreated message) {
        entrySaveService.save(new EntrySaveCommand(message.eventId(),
                message.dropId(), message.userId(), message.entrySeq(), message.appliedAt()));
    }
}
