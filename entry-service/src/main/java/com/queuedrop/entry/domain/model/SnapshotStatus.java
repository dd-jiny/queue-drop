package com.queuedrop.entry.domain.model;

/** 추첨 진행 상태. 4단계는 OPEN 만 쓰고 CLOSING / DRAWN 은 5단계에서 쓴다. */
public enum SnapshotStatus { OPEN, CLOSING, DRAWN }
