package com.queuedrop.ranking.presentation.listener;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.ranking.application.dto.RankingRecordCommand;
import com.queuedrop.ranking.application.service.RankingAggregateService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** entry.created → 랭킹 집계. 검증은 application, Redis 장애는 전파해 KafkaConsumerConfig 재시도에 맡긴다 */
@Component
@RequiredArgsConstructor
class EntryCreatedListener {

    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.ENTRY_CREATED)
    void on(EntryCreated message) {
        aggregateService.recordEntry(
                new RankingRecordCommand(message.dropId(), message.userId(), message.appliedAt()));
    }
}
