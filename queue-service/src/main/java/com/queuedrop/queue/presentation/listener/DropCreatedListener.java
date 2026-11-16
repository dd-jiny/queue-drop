package com.queuedrop.queue.presentation.listener;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.queue.application.dto.QueueMetaRegisterCommand;
import com.queuedrop.queue.application.service.QueueMetaRegisterService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 멱등: 재수신 시 meta·active-drops 를 같은 값으로 덮어쓰고 대기열은 건드리지 않는다. group-id 는 공통 설정 */
@Component
@RequiredArgsConstructor
public class DropCreatedListener {

    private final QueueMetaRegisterService registerService;

    @KafkaListener(topics = Topics.DROP_CREATED)
    public void on(DropCreated message) {
        registerService.register(new QueueMetaRegisterCommand(message.dropId(), message.openAt(), message.closeAt()));
    }
}
