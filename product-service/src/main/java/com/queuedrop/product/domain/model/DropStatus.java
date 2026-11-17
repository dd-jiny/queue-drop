package com.queuedrop.product.domain.model;

/** 드로우 상태. DRAWN 만 저장 값이고 나머지는 시각으로 계산한다. */
public enum DropStatus {
    SCHEDULED, OPEN, CLOSED, DRAWN
}
