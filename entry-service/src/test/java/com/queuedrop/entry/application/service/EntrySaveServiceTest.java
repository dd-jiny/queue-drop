package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.application.service.fake.InMemoryEntryRepository;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EntrySaveServiceTest {

    private static final long DROP_ID = 10L;
    private static final long USER_ID = 7L;
    private static final Instant APPLIED_AT = Instant.parse("2026-11-16T10:30:00Z");

    private InMemoryEntryRepository entryRepository;
    private FakeEntryGate entryGate;
    private EntrySaveService service;

    @BeforeEach
    void setUp() {
        entryRepository = new InMemoryEntryRepository();
        entryGate = new FakeEntryGate();
        service = new EntrySaveService(entryRepository, entryGate);
    }

    @Test
    @DisplayName("[T4-07] Redis 응모자에 없는 사용자의 이벤트(보상된 유령 이벤트)는 저장하지 않는다")
    void ignoresGhostEvent() {
        // Given: 응모자로 등록하지 않음 (보상으로 빠진 상태)
        EntrySaveCommand command = new EntrySaveCommand(DROP_ID, USER_ID, 1, APPLIED_AT);

        // When
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isZero();
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID)).isEmpty();
    }

    @Test
    @DisplayName("[T4-07] 다른 드로우의 응모자여도 해당 드로우 응모자가 아니면 저장하지 않는다")
    void ignoresApplicantOfOtherDrop() {
        // Given
        entryGate.givenApplicant(DROP_ID + 1, USER_ID);

        // When
        service.save(new EntrySaveCommand(DROP_ID, USER_ID, 1, APPLIED_AT));

        // Then
        assertThat(entryRepository.count()).isZero();
    }

    @Test
    @DisplayName("[T4-08] 응모자의 이벤트는 APPLIED 로 저장되고 필드가 명령과 같다")
    void savesApplicant() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID);

        // When
        service.save(new EntrySaveCommand(DROP_ID, USER_ID, 3, APPLIED_AT));

        // Then
        Entry saved = entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow();
        assertThat(saved.getEntrySeq()).isEqualTo(3);
        assertThat(saved.getStatus()).isEqualTo(EntryStatus.APPLIED);
        assertThat(saved.getAppliedAt()).isEqualTo(APPLIED_AT);
    }

    @Test
    @DisplayName("[T4-08] 같은 이벤트를 2번 처리해도 저장소는 1건, 예외 없음 (재전달 멱등)")
    void idempotentOnRedelivery() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID);
        EntrySaveCommand command = new EntrySaveCommand(DROP_ID, USER_ID, 1, APPLIED_AT);

        // When
        service.save(command);
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("[T4-08] 같은 사용자의 다른 seq 이벤트(유령 + 재응모)는 무시되고 첫 저장 값이 유지된다")
    void keepsFirstOnSameUserDifferentSeq() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID);
        service.save(new EntrySaveCommand(DROP_ID, USER_ID, 1, APPLIED_AT));

        // When
        assertThatCode(() -> service.save(new EntrySaveCommand(DROP_ID, USER_ID, 2, APPLIED_AT.plusSeconds(1))))
                .doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow().getEntrySeq())
                .isEqualTo(1);
    }

    @Test
    @DisplayName("[T4-07] 저장 후 응모자에서 빠진 상태로 같은 이벤트가 재전달되면 무시하고 기존 1건을 지우지 않는다")
    void redeliveryAfterGateRemovalKeepsSavedEntry() {
        // Given: 정상 저장 후 Gate 에서 응모자가 빠짐 (Redis 만료·초기화 등)
        entryGate.givenApplicant(DROP_ID, USER_ID);
        EntrySaveCommand command = new EntrySaveCommand(DROP_ID, USER_ID, 1, APPLIED_AT);
        service.save(command);
        entryGate.compensate(DROP_ID, USER_ID);

        // When
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow().getEntrySeq()).isEqualTo(1);
    }
}
