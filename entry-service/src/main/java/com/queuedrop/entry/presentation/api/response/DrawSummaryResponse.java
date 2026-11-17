package com.queuedrop.entry.presentation.api.response;

import com.queuedrop.entry.application.dto.DrawSummaryResult;
import java.time.Instant;

/** seed 는 문자열: JavaScript Number 는 2^53 을 넘는 long 을 정확히 표현하지 못한다 */
public record DrawSummaryResponse(Long dropId, int applicantCount, int winnerCount, String seed, Instant drawnAt) {

    public static DrawSummaryResponse from(DrawSummaryResult r) {
        return new DrawSummaryResponse(r.dropId(), r.applicantCount(), r.winnerCount(), Long.toString(r.seed()), r.drawnAt());
    }
}
