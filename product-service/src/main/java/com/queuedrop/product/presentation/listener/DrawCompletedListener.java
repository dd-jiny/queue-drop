package com.queuedrop.product.presentation.listener;

import com.queuedrop.common.event.DrawCompleted;
import com.queuedrop.common.event.Topics;
import com.queuedrop.product.application.dto.MarkDrawnCommand;
import com.queuedrop.product.application.service.DropService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 예외를 잡지 않는다: 재시도·DLT 분류는 KafkaConsumerConfig 의 DefaultErrorHandler 가 한다
 * (PRODUCT_DROP_NOT_FOUND 는 재시도 없이 커밋, 그 밖은 재시도 후 draw.completed.DLT).
 */
@Component
@RequiredArgsConstructor
public class DrawCompletedListener {

    private final DropService dropService;

    @KafkaListener(topics = Topics.DRAW_COMPLETED)   // group-id: spring.kafka.consumer.group-id
    public void on(DrawCompleted message) {
        dropService.markDrawn(new MarkDrawnCommand(message.dropId()));
    }
}
