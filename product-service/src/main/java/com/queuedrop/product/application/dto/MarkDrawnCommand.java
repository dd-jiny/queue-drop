package com.queuedrop.product.application.dto;

/** draw.completed 수신 시 드로우 추첨 완료 반영 (5단계 3.5) */
public record MarkDrawnCommand(Long dropId) {}
