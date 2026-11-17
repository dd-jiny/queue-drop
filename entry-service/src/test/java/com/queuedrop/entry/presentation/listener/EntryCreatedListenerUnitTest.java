package com.queuedrop.entry.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.queuedrop.common.event.EntryCreated;
import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.service.EntrySaveService;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * EntryCreatedListener 의 메시지 → 명령 변환 단위 테스트 (v1.5: eventId 전달).
 * Redis·Kafka 를 거친 eventId 의미 검증은 통합 테스트 T4-26 (커밋 #10b) 에서 한다.
 */
class EntryCreatedListenerUnitTest {

    @Test
    @DisplayName("[T4-26 단위] EntryCreated 의 eventId·dropId·userId·entrySeq·appliedAt 이 그대로 EntrySaveCommand 로 전달된다")
    void passesEventIdToSaveCommand() {
        // Given
        EntrySaveService saveService = mock(EntrySaveService.class);
        EntryCreatedListener listener = new EntryCreatedListener(saveService);
        Instant appliedAt = Instant.parse("2026-11-20T10:30:00.123456Z");
        EntryCreated message = new EntryCreated("evt-123", 1L, 2L, 3, appliedAt);

        // When
        listener.on(message);

        // Then
        ArgumentCaptor<EntrySaveCommand> captor = ArgumentCaptor.forClass(EntrySaveCommand.class);
        verify(saveService).save(captor.capture());
        assertThat(captor.getValue()).isEqualTo(new EntrySaveCommand("evt-123", 1L, 2L, 3, appliedAt));
    }
}
