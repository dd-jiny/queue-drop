package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.MyEntryResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.application.service.fake.InMemoryEntryRepository;
import com.queuedrop.entry.domain.model.Entry;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EntryQueryServiceTest {

    private static final long DROP_ID = 10L;
    private static final long USER_ID = 7L;
    private static final Instant APPLIED_AT = Instant.parse("2026-11-16T10:30:00Z");

    private InMemoryEntryRepository entryRepository;
    private FakeEntryGate entryGate;
    private EntryQueryService service;

    @BeforeEach
    void setUp() {
        entryRepository = new InMemoryEntryRepository();
        entryGate = new FakeEntryGate();
        service = new EntryQueryService(entryRepository, entryGate);
    }

    @Test
    @DisplayName("[T4-10] DB 에 응모가 있으면 DB 값을 돌려준다")
    void returnsDbEntry() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(DROP_ID, USER_ID, 5, APPLIED_AT));

        // When
        MyEntryResult result = service.myEntry(DROP_ID, USER_ID);

        // Then
        assertThat(result).isEqualTo(new MyEntryResult(DROP_ID, 5, "APPLIED", APPLIED_AT));
    }

    @Test
    @DisplayName("[T4-10] DB 에 없고 Gate 응모자이면 저장 대기로 보고 status=APPLIED, entrySeq·appliedAt=null")
    void returnsPendingSaveWhenOnlyInGate() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID);

        // When
        MyEntryResult result = service.myEntry(DROP_ID, USER_ID);

        // Then
        assertThat(result.dropId()).isEqualTo(DROP_ID);
        assertThat(result.status()).isEqualTo("APPLIED");
        assertThat(result.entrySeq()).isNull();
        assertThat(result.appliedAt()).isNull();
    }

    @Test
    @DisplayName("[T4-10] DB 에도 Gate 에도 없으면 ApplicationException(NOT_FOUND)")
    void throwsNotFoundWhenNowhere() {
        // Given: 다른 사용자·다른 드로우 응모만 있음
        entryGate.givenApplicant(DROP_ID, USER_ID + 1);
        entryRepository.saveIfAbsent(Entry.apply(DROP_ID + 1, USER_ID, 1, APPLIED_AT));

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> service.myEntry(DROP_ID, USER_ID))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(EntryErrorCode.NOT_FOUND));
    }

    @Test
    @DisplayName("[T4-10] 목록 조회는 DB 의 내 응모만 최신순으로 돌려주고 Redis 로 보완하지 않는다")
    void myEntriesFromDbOnly() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(1L, USER_ID, 1, APPLIED_AT));
        entryRepository.saveIfAbsent(Entry.apply(2L, USER_ID, 1, APPLIED_AT.plusSeconds(60)));
        entryRepository.saveIfAbsent(Entry.apply(1L, USER_ID + 1, 2, APPLIED_AT));
        entryGate.givenApplicant(3L, USER_ID); // 저장 대기 중 — 목록에는 안 나옴

        // When
        List<MyEntryResult> results = service.myEntries(USER_ID);

        // Then
        assertThat(results).extracting(MyEntryResult::dropId).containsExactly(2L, 1L);
        assertThat(results).allSatisfy(r -> assertThat(r.status()).isEqualTo("APPLIED"));
    }

    @Test
    @DisplayName("[T4-10] 응모가 없으면 목록 조회는 빈 목록")
    void myEntriesEmpty() {
        // When / Then
        assertThat(service.myEntries(USER_ID)).isEmpty();
    }

    @Test
    @DisplayName("[T4-10] DB 와 Gate 에 모두 있으면 DB 값이 우선한다 (entrySeq·appliedAt 이 채워짐)")
    void prefersDbOverGate() {
        // Given
        entryRepository.saveIfAbsent(Entry.apply(DROP_ID, USER_ID, 5, APPLIED_AT));
        entryGate.givenApplicant(DROP_ID, USER_ID);

        // When
        MyEntryResult result = service.myEntry(DROP_ID, USER_ID);

        // Then
        assertThat(result).isEqualTo(new MyEntryResult(DROP_ID, 5, "APPLIED", APPLIED_AT));
    }
}
