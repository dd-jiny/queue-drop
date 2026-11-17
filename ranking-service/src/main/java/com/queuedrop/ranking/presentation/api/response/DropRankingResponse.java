package com.queuedrop.ranking.presentation.api.response;

import com.queuedrop.ranking.application.dto.DropRankResult;

public record DropRankingResponse(long rank, long dropId, String title, long entryCount) {

    public static DropRankingResponse from(DropRankResult r) {
        return new DropRankingResponse(r.rank(), r.dropId(), r.title(), r.entryCount());
    }
}
