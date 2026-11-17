package com.queuedrop.entry.application.dto;

/** 관리자 수동 추첨 (D5-1) */
public record ManualDrawCommand(Requester requester, Long dropId) {}
