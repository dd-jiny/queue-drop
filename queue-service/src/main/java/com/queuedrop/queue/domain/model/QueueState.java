package com.queuedrop.queue.domain.model;

/** 사용자 대기열 상태. 판정 우선순위: SOLD_OUT > ADMITTED > WAITING > NOT_IN_QUEUE */
public enum QueueState { WAITING, ADMITTED, SOLD_OUT, NOT_IN_QUEUE }
