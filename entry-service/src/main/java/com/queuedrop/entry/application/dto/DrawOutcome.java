package com.queuedrop.entry.application.dto;

/** DrawTransaction 실행 결과. domain 타입을 밖으로 내보내지 않기 위한 application DTO */
public record DrawOutcome(boolean drawn, DrawCompletedPayload payload) {

    public static DrawOutcome skipped() {
        return new DrawOutcome(false, null);
    }
}
