package com.queuedrop.entry.presentation.api;

import com.queuedrop.entry.application.dto.ManualDrawCommand;
import com.queuedrop.entry.application.dto.Requester;
import com.queuedrop.entry.application.service.DrawService;
import com.queuedrop.entry.presentation.api.response.DrawSummaryResponse;
import java.time.Clock;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** CLOSING 장기화 시 관리자 수동 추첨 (D5-1) */
@RestController
@RequestMapping("/api/entries/admin/drops/{dropId:\\d+}")
@RequiredArgsConstructor
public class AdminDrawController {

    private final DrawService drawService;
    private final Clock clock;

    @PostMapping("/draw")
    public DrawSummaryResponse draw(@PathVariable Long dropId,
                                    @RequestHeader("X-User-Id") Long userId,
                                    @RequestHeader("X-User-Role") String role) {
        return DrawSummaryResponse.from(
                drawService.drawManually(new ManualDrawCommand(new Requester(userId, role), dropId), clock.instant()));
    }
}
