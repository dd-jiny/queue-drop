package com.queuedrop.entry.application.dto;

import com.queuedrop.entry.domain.model.DrawResult;
import java.time.Instant;

public record DrawSummaryResult(Long dropId, int applicantCount, int winnerCount, long seed, Instant drawnAt) {

    /** service 패키지에서 호출. presentation 은 domain 을 import 하지 않아 쓸 수 없다 */
    public static DrawSummaryResult from(DrawResult r) {
        return new DrawSummaryResult(r.getDropId(), r.getApplicantCount(), r.getWinnerCount(), r.getSeed(),
                r.getDrawnAt());
    }
}
