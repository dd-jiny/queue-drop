package com.queuedrop.ranking.domain.exception;

/**
 * 랭킹 저장소(Redis) 호출 실패 — 연결 실패·명령 타임아웃 등. 순수 Java.
 * DomainException 을 상속하지 않는다: 리스너에서 스킵되지 않고 전파되어 재시도 대상이 되어야 한다.
 */
public class RankingStoreUnavailableException extends RuntimeException {
    public RankingStoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
