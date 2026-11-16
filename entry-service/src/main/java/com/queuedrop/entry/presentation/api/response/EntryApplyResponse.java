package com.queuedrop.entry.presentation.api.response;

import com.queuedrop.entry.application.dto.EntryApplyResult;

public record EntryApplyResponse(int entrySeq) {

    public static EntryApplyResponse from(EntryApplyResult result) {
        return new EntryApplyResponse(result.entrySeq());
    }
}
