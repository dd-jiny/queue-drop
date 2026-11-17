package com.queuedrop.queue.presentation.api;

import com.queuedrop.queue.application.dto.QueueEnterCommand;
import com.queuedrop.queue.application.dto.QueueStatusQuery;
import com.queuedrop.queue.application.service.QueueService;
import com.queuedrop.queue.presentation.api.response.QueueStatusResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 진입은 신규·재진입 모두 200 (멱등) */
@RestController
@RequestMapping("/api/queue/drops")
@RequiredArgsConstructor
public class QueueController {

    private final QueueService queueService;

    @PostMapping("/{dropId:\\d+}")
    public QueueStatusResponse enter(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return QueueStatusResponse.from(queueService.enter(new QueueEnterCommand(dropId, userId)));
    }

    @GetMapping("/{dropId:\\d+}")
    public QueueStatusResponse status(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return QueueStatusResponse.from(queueService.status(new QueueStatusQuery(dropId, userId)));
    }
}
