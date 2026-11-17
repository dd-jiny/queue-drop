package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.application.dto.EntrySaveCommand;
import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.application.service.fake.InMemoryEntryRepository;
import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EntrySaveServiceTest {

    private static final long DROP_ID = 10L;
    private static final long USER_ID = 7L;
    private static final String EVENT_ID = "e-1";
    private static final Instant APPLIED_AT = Instant.parse("2026-11-16T10:30:00Z");
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");

    private InMemoryEntryRepository entryRepository;
    private FakeEntryGate entryGate;
    private EntrySaveService service;

    @BeforeEach
    void setUp() {
        entryRepository = new InMemoryEntryRepository();
        entryGate = new FakeEntryGate();
        service = new EntrySaveService(entryRepository, entryGate);
    }

    private static EntrySaveCommand command(String eventId, int seq) {
        return new EntrySaveCommand(eventId, DROP_ID, USER_ID, seq, APPLIED_AT);
    }

    @Test
    @DisplayName("[T4-07] Redis 응모자에 없는 사용자의 이벤트(보상된 유령 이벤트)는 저장하지 않고, claim 에 command.eventId 를 넘긴다")
    void ignoresGhostEvent() {
        // Given: 응모자로 등록하지 않음 (보상으로 빠진 상태)
        EntrySaveCommand command = command(EVENT_ID, 1);

        // When
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isZero();
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID)).isEmpty();
        assertThat(entryGate.claimCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, EVENT_ID));
        assertThat(entryGate.persisted(DROP_ID)).isEmpty();
    }

    @Test
    @DisplayName("[T4-07] 응모자이지만 requests 의 eventId 가 다르면(이전 시도의 유령 이벤트) 저장하지 않는다")
    void ignoresEventOfOtherRequest() {
        // Given: 현재 응모는 e-2 요청이 잡은 자리
        entryGate.givenApplicant(DROP_ID, USER_ID, "e-2");

        // When
        assertThatCode(() -> service.save(command("e-1", 1))).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isZero();
        assertThat(entryGate.claimCalls()).containsExactly(new FakeEntryGate.Call(DROP_ID, USER_ID, "e-1"));
        assertThat(entryGate.persisted(DROP_ID)).isEmpty();
    }

    @Test
    @DisplayName("[T4-07] 다른 드로우의 응모자여도 해당 드로우 응모자가 아니면 저장하지 않는다")
    void ignoresApplicantOfOtherDrop() {
        // Given
        entryGate.givenApplicant(DROP_ID + 1, USER_ID, EVENT_ID);

        // When
        service.save(command(EVENT_ID, 1));

        // Then
        assertThat(entryRepository.count()).isZero();
    }

    @Test
    @DisplayName("[T4-07] 판정(e1) → 보상(e1) → 재응모(e2) 후: e1 의 유령 이벤트는 무시되고 e2 이벤트는 재응모 번호로 저장된다")
    void ghostOfPreviousAttemptIgnoredAfterReapply() {
        // Given — 실제 판정 경로
        entryGate.initialize(DROP_ID, 3, OPEN_AT, CLOSE_AT);
        entryGate.grantToken(DROP_ID, USER_ID);
        int firstSeq = entryGate.tryApply(DROP_ID, USER_ID, "e1", APPLIED_AT).entrySeq();
        assertThat(entryGate.compensate(DROP_ID, USER_ID, "e1")).isEqualTo(CompensationResult.COMPENSATED);
        int secondSeq = entryGate.tryApply(DROP_ID, USER_ID, "e2", APPLIED_AT.plusSeconds(1)).entrySeq();

        // When — 유령 이벤트가 먼저, 재응모 이벤트가 나중에 온다
        service.save(command("e1", firstSeq));
        service.save(new EntrySaveCommand("e2", DROP_ID, USER_ID, secondSeq, APPLIED_AT.plusSeconds(1)));

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow().getEntrySeq())
                .isEqualTo(secondSeq);
        assertThat(entryGate.persisted(DROP_ID)).containsExactly(USER_ID);
    }

    @Test
    @DisplayName("[T4-08] 같은 eventId 로 판정된 응모자의 이벤트는 APPLIED 로 저장되고 필드가 명령과 같으며 persisted 에 들어간다")
    void savesApplicant() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID, EVENT_ID);

        // When
        service.save(command(EVENT_ID, 3));

        // Then
        Entry saved = entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow();
        assertThat(saved.getEntrySeq()).isEqualTo(3);
        assertThat(saved.getStatus()).isEqualTo(EntryStatus.APPLIED);
        assertThat(saved.getAppliedAt()).isEqualTo(APPLIED_AT);
        assertThat(entryGate.persisted(DROP_ID)).containsExactly(USER_ID);
    }

    @Test
    @DisplayName("[T4-08] 같은 이벤트를 2번 처리해도 저장소는 1건, 예외 없음 (재전달 멱등, claim 도 두 번 다 통과)")
    void idempotentOnRedelivery() {
        // Given
        entryGate.givenApplicant(DROP_ID, USER_ID, EVENT_ID);
        EntrySaveCommand command = command(EVENT_ID, 1);

        // When
        service.save(command);
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryGate.claimCalls()).hasSize(2);
    }

    @Test
    @DisplayName("[T4-08] 저장 확정 뒤에는 같은 eventId 의 보상이 ALREADY_PERSISTED 로 거절되고, 재전달도 1건 그대로다")
    void compensationRejectedAfterClaim() {
        // Given: 정상 저장
        entryGate.initialize(DROP_ID, 3, OPEN_AT, CLOSE_AT);
        entryGate.grantToken(DROP_ID, USER_ID);
        int seq = entryGate.tryApply(DROP_ID, USER_ID, EVENT_ID, APPLIED_AT).entrySeq();
        EntrySaveCommand command = command(EVENT_ID, seq);
        service.save(command);

        // When — 발행 실패로 판단한 응모 서비스의 보상이 뒤늦게 도착, 이어서 같은 이벤트 재전달
        CompensationResult compensation = entryGate.compensate(DROP_ID, USER_ID, EVENT_ID);
        assertThatCode(() -> service.save(command)).doesNotThrowAnyException();

        // Then — Redis 응모자 유지 (DB 저장 ⇔ Redis 응모자), DB 1건
        assertThat(compensation).isEqualTo(CompensationResult.ALREADY_PERSISTED);
        assertThat(entryGate.applicants(DROP_ID)).containsExactly(USER_ID);
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(2);
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow().getEntrySeq()).isEqualTo(seq);
    }

    @Test
    @DisplayName("[T4-08] claim 이 통과한 뒤 저장이 실패(seq 충돌)하면 예외를 전파한다 — 재시도·DLT 대상, 확정은 유지")
    void saveFailureAfterClaimPropagates() {
        // Given — 다른 사용자가 같은 seq 1 로 이미 저장돼 있다
        long otherUser = USER_ID + 1;
        entryGate.givenApplicant(DROP_ID, otherUser, "e-other");
        service.save(new EntrySaveCommand("e-other", DROP_ID, otherUser, 1, APPLIED_AT));
        entryGate.givenApplicant(DROP_ID, USER_ID, EVENT_ID);

        // When / Then
        assertThatThrownBy(() -> service.save(command(EVENT_ID, 1))).isInstanceOf(RuntimeException.class);
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryGate.persisted(DROP_ID)).contains(USER_ID);
        assertThat(entryGate.claimForSave(DROP_ID, USER_ID, EVENT_ID)).as("재처리 시 claim 은 다시 true").isTrue();
    }

    @Test
    @DisplayName("[T4-08] 보상 후 재응모(e2)가 저장된 뒤 늦게 온 e1 유령 이벤트는 무시되고 저장 값이 유지된다")
    void lateGhostAfterReapplySavedKeepsEntry() {
        // Given — e2 로 저장 확정
        entryGate.givenApplicant(DROP_ID, USER_ID, "e2");
        service.save(new EntrySaveCommand("e2", DROP_ID, USER_ID, 2, APPLIED_AT.plus(Duration.ofSeconds(1))));

        // When — e1(seq 1) 유령 이벤트
        assertThatCode(() -> service.save(command("e1", 1))).doesNotThrowAnyException();

        // Then
        assertThat(entryRepository.count()).isEqualTo(1);
        assertThat(entryRepository.findByDropIdAndUserId(DROP_ID, USER_ID).orElseThrow().getEntrySeq())
                .isEqualTo(2);
    }
}
