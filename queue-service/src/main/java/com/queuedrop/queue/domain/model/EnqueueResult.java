package com.queuedrop.queue.domain.model;

/** 진입 결과. admitted=true 면 이미 토큰 보유(rank=-1). 아니면 rank = 0-based 순번(내 앞 인원 수). */
public record EnqueueResult(boolean admitted, long rank) {

    public static EnqueueResult alreadyAdmitted() {
        return new EnqueueResult(true, -1);
    }

    public static EnqueueResult waiting(long rank) {
        return new EnqueueResult(false, rank);
    }
}
