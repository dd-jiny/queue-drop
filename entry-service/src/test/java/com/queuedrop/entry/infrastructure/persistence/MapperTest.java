package com.queuedrop.entry.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** static Mapper 순수 단위 테스트 (DB 없이 필드 누락·뒤바뀜 검출). */
class MapperTest {

    private static final Instant OPEN_AT = Instant.parse("2026-11-17T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-17T12:00:00Z");

    @Test
    @DisplayName("[T4-22] DropSnapshotMapper toEntity → toDomain 왕복 시 모든 필드가 같다")
    void dropSnapshotRoundTrip() {
        // Given: 필드 값이 서로 달라 뒤바뀌면 드러나도록 한다
        DropSnapshot origin = DropSnapshot.restore(3L, 200, 20, OPEN_AT, CLOSE_AT, SnapshotStatus.DRAWN);

        // When
        DropSnapshot back = DropSnapshotMapper.toDomain(DropSnapshotMapper.toEntity(origin));

        // Then
        assertThat(back).usingRecursiveComparison().isEqualTo(origin);
    }

    @Test
    @DisplayName("[T4-21] EntryMapper toDomain 은 엔티티 필드를 그대로 옮긴다")
    void entryToDomain() {
        // Given
        EntryJpaEntity entity = new EntryJpaEntity(11L, 1L, 2L, 3, EntryStatus.WON, OPEN_AT, CLOSE_AT);

        // When
        Entry entry = EntryMapper.toDomain(entity);

        // Then
        assertThat(entry.getId()).isEqualTo(11L);
        assertThat(entry.getDropId()).isEqualTo(1L);
        assertThat(entry.getUserId()).isEqualTo(2L);
        assertThat(entry.getEntrySeq()).isEqualTo(3);
        assertThat(entry.getStatus()).isEqualTo(EntryStatus.WON);
        assertThat(entry.getAppliedAt()).isEqualTo(OPEN_AT);
    }
}
