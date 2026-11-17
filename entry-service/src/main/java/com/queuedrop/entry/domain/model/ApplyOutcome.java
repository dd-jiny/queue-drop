package com.queuedrop.entry.domain.model;

/** 응모 판정 결과. 규칙 자체는 Lua 에 있고 domain 은 결과 타입만 갖는다. */
public enum ApplyOutcome { OK, CLOSED, NO_TOKEN, DUPLICATE, SOLD_OUT }
