package com.queuedrop.entry.application.dto;

import java.time.Instant;

/** dropId 는 Long: 메시지의 null 을 리스너에서 언박싱하다 NPE(→ 재시도·DLT)가 나지 않고 도메인 검증까지 오게 한다 */
public record DropSnapshotRegisterCommand(
        Long dropId, int entryCapacity, int winnerCount, Instant openAt, Instant closeAt) {}
