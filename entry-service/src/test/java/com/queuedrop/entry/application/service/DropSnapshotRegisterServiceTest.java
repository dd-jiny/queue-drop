package com.queuedrop.entry.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.entry.application.dto.DropSnapshotRegisterCommand;
import com.queuedrop.entry.application.service.fake.FakeEntryGate;
import com.queuedrop.entry.application.service.fake.FakeEntryGate.InitializeCall;
import com.queuedrop.entry.application.service.fake.InMemoryDropSnapshotRepository;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DropSnapshotRegisterServiceTest {

    private static final Long DROP_ID = 10L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");

    private InMemoryDropSnapshotRepository snapshotRepository;
    private FakeEntryGate entryGate;
    private DropSnapshotRegisterService service;

    @BeforeEach
    void setUp() {
        snapshotRepository = new InMemoryDropSnapshotRepository();
        entryGate = new FakeEntryGate();
        service = new DropSnapshotRegisterService(snapshotRepository, entryGate);
    }

    @Test
    @DisplayName("[T4-09] 정상 명령이면 스냅샷을 OPEN 으로 저장하고 Gate 를 정원으로 초기화한다")
    void registersValidSnapshot() {
        // Given
        DropSnapshotRegisterCommand command = new DropSnapshotRegisterCommand(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT);

        // When
        service.register(command);

        // Then
        DropSnapshot saved = snapshotRepository.findById(DROP_ID).orElseThrow();
        assertThat(saved.getEntryCapacity()).isEqualTo(100);
        assertThat(saved.getWinnerCount()).isEqualTo(10);
        assertThat(saved.getStatus()).isEqualTo(SnapshotStatus.OPEN);
        assertThat(entryGate.initializeCalls())
                .containsExactly(new InitializeCall(DROP_ID, 100, OPEN_AT, CLOSE_AT));
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(100);
    }

    @Test
    @DisplayName("[T4-09] 이미 저장된 스냅샷이면 저장소는 첫 값 그대로이고, Gate.initialize 는 다시 호출된다 (Redis 복구 기회)")
    void redeliveryKeepsSnapshotButInitializesGate() {
        // Given: DB 에만 저장된 상태 (이전 처리에서 Redis 초기화 실패 가정)
        snapshotRepository.saveIfAbsent(DropSnapshot.register(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT));

        // When: 같은 드로우 재수신 (정원·기간이 달라도 첫 값 유지)
        service.register(new DropSnapshotRegisterCommand(
                DROP_ID, 200, 10, OPEN_AT.plusSeconds(60), CLOSE_AT.plusSeconds(60)));

        // Then
        assertThat(snapshotRepository.count()).isEqualTo(1);
        assertThat(snapshotRepository.findById(DROP_ID).orElseThrow().getEntryCapacity()).isEqualTo(100);
        // Gate 는 메시지 값(200)이 아니라 저장된 스냅샷 값으로 초기화된다 (v1.3)
        assertThat(entryGate.initializeCalls())
                .containsExactly(new InitializeCall(DROP_ID, 100, OPEN_AT, CLOSE_AT));
    }

    static Stream<Arguments> invalidCommands() {
        return Stream.of(
                Arguments.of("capacity < winner", new DropSnapshotRegisterCommand(DROP_ID, 5, 10, OPEN_AT, CLOSE_AT)),
                Arguments.of("winner = 0", new DropSnapshotRegisterCommand(DROP_ID, 5, 0, OPEN_AT, CLOSE_AT)),
                Arguments.of("openAt == closeAt", new DropSnapshotRegisterCommand(DROP_ID, 100, 10, OPEN_AT, OPEN_AT)),
                Arguments.of("openAt > closeAt", new DropSnapshotRegisterCommand(DROP_ID, 100, 10, CLOSE_AT, OPEN_AT)),
                Arguments.of("dropId null", new DropSnapshotRegisterCommand(null, 100, 10, OPEN_AT, CLOSE_AT)),
                Arguments.of("openAt null", new DropSnapshotRegisterCommand(DROP_ID, 100, 10, null, CLOSE_AT)),
                Arguments.of("closeAt null", new DropSnapshotRegisterCommand(DROP_ID, 100, 10, OPEN_AT, null)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCommands")
    @DisplayName("[T4-09] 잘못된 drop.created 는 예외 없이 스킵한다 — 저장소 0건, Gate.initialize 호출 0")
    void skipsInvalidCommand(String caseName, DropSnapshotRegisterCommand command) {
        // When / Then
        assertThatCode(() -> service.register(command)).doesNotThrowAnyException();
        assertThat(snapshotRepository.count()).isZero();
        assertThat(entryGate.initializeCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T4-09] Redis 초기화가 실패하면 예외가 전파되고(재시도 대상), 재전달 시 DB 는 그대로·Redis 는 복구된다")
    void gateFailureIsPropagatedAndRecoveredOnRedelivery() {
        // Given: 첫 처리에서 Redis 초기화 실패
        DropSnapshotRegisterCommand command = new DropSnapshotRegisterCommand(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT);
        entryGate.failNextInitializeWith(new IllegalStateException("redis down"));

        // When: 첫 수신은 실패해야 리스너가 재시도·DLT 로 보낸다 (스킵으로 삼키면 안 됨)
        assertThatThrownBy(() -> service.register(command)).isInstanceOf(IllegalStateException.class);

        // Then: DB 는 이미 저장됨
        assertThat(snapshotRepository.count()).isEqualTo(1);

        // When: 재전달
        assertThatCode(() -> service.register(command)).doesNotThrowAnyException();

        // Then: DB 1건 유지, Redis 정원 초기화 완료
        assertThat(snapshotRepository.count()).isEqualTo(1);
        assertThat(entryGate.initializeCalls()).hasSize(2);
        assertThat(entryGate.remaining(DROP_ID)).isEqualTo(100);
    }

    @Test
    @DisplayName("[T4-09] 스냅샷 저장이 실패하면 예외가 전파되고 Gate.initialize 는 호출되지 않는다")
    void repositoryFailurePropagatesWithoutGateInit() {
        // Given
        snapshotRepository.failSaveWith(new IllegalStateException("db down"));

        // When / Then
        assertThatThrownBy(() -> service.register(
                new DropSnapshotRegisterCommand(DROP_ID, 100, 10, OPEN_AT, CLOSE_AT)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(entryGate.initializeCalls()).isEmpty();
    }
}
