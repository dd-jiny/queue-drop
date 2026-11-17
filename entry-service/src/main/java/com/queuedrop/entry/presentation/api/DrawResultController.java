package com.queuedrop.entry.presentation.api;

import com.queuedrop.entry.application.service.DrawQueryService;
import com.queuedrop.entry.presentation.api.response.DrawSummaryResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 추첨 결과 공개 조회. 추첨 전·없는 dropId 모두 ENTRY_DRAW_NOT_COMPLETED (409) */
@RestController
@RequestMapping("/api/entries/drops/{dropId:\\d+}")
@RequiredArgsConstructor
public class DrawResultController {

    private final DrawQueryService queryService;

    @GetMapping("/result")
    public DrawSummaryResponse summary(@PathVariable Long dropId) {
        return DrawSummaryResponse.from(queryService.summary(dropId));
    }
}
