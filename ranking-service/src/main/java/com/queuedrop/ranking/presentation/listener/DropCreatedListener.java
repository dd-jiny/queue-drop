package com.queuedrop.ranking.presentation.listener;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.ranking.application.dto.DropInfoCommand;
import com.queuedrop.ranking.application.service.RankingAggregateService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** drop.created → 드로우 제목·마감 시각 저장 (HSET 덮어쓰기라 재전달에 멱등) */
@Component
@RequiredArgsConstructor
class DropCreatedListener {

    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.DROP_CREATED)
    void on(DropCreated message) {
        aggregateService.registerDrop(
                new DropInfoCommand(message.dropId(), message.title(), message.closeAt()));
    }
}
