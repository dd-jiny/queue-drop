package com.queuedrop.queue.application.port;

import java.time.Duration;

/** 다중 인스턴스에서 입장 처리가 주기(window)당 1회만 실행되도록 하는 전역 게이트 */
public interface SchedulerGate {
    boolean tryPass(String name, Duration window);
}
