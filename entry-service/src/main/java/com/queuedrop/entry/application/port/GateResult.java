package com.queuedrop.entry.application.port;

import com.queuedrop.entry.domain.model.ApplyOutcome;

/** entrySeq 는 outcome == OK 일 때만 값이 있다. */
public record GateResult(ApplyOutcome outcome, Integer entrySeq) {

    public static GateResult ok(int entrySeq) {
        return new GateResult(ApplyOutcome.OK, entrySeq);
    }

    public static GateResult rejected(ApplyOutcome outcome) {
        return new GateResult(outcome, null);
    }

    public boolean isOk() {
        return outcome == ApplyOutcome.OK;
    }
}
