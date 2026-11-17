package com.queuedrop.entry.application.port;

/** Redis 호출 실패 (연결 실패·명령 타임아웃). 타임아웃이면 Lua 가 실행됐는지 알 수 없다 (E-41) */
public class GateUnavailableException extends RuntimeException {

    public GateUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
