package com.queuedrop.queue.application.config;

/** application 계층 설정 값 객체. 바인딩·검증은 infrastructure/config 에서 한다 */
public record AdmissionProperties(long intervalMs, int batchSize, long tokenTtlSeconds) {

    /**
     * 전역 게이트 유지 시간 = 주기의 90%.
     * Redis 는 now &gt; expireAt 일 때만 만료로 보므로 window 를 주기와 같게 두면 다음 틱이 조금만 일찍 도착해도
     * 게이트가 남아 있어 그 주기를 건너뛴다(단일 인스턴스에서도 약 40%). 여유 10% 로 이를 막고,
     * 다중 인스턴스 상한은 "window 당 1회"(초당 최대 약 1.11N명)가 된다.
     */
    public long gateWindowMs() {
        return Math.max(1, intervalMs * 9 / 10);
    }
}
