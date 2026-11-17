package com.queuedrop.ranking.application.dto;

import java.time.Instant;

/** drop.created 필드를 그대로 담는다 (박싱 타입). 검증은 application 이 한다 */
public record DropInfoCommand(Long dropId, String title, Instant closeAt) {}
