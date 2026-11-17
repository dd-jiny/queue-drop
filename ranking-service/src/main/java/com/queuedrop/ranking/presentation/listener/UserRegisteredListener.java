package com.queuedrop.ranking.presentation.listener;

import com.queuedrop.common.event.Topics;
import com.queuedrop.common.event.UserRegistered;
import com.queuedrop.ranking.application.dto.UserInfoCommand;
import com.queuedrop.ranking.application.service.RankingAggregateService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** user.registered → 닉네임 저장 (HSET 덮어쓰기라 재전달에 멱등) */
@Component
@RequiredArgsConstructor
class UserRegisteredListener {

    private final RankingAggregateService aggregateService;

    @KafkaListener(topics = Topics.USER_REGISTERED)
    void on(UserRegistered message) {
        aggregateService.registerUser(new UserInfoCommand(message.userId(), message.nickname()));
    }
}
