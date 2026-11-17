package com.queuedrop.entry.application.port;

/** 추첨 시드 소스. 테스트에서 고정 시드를 주입하기 위한 포트 (구현 결정) */
public interface SeedGenerator {

    long next();
}
