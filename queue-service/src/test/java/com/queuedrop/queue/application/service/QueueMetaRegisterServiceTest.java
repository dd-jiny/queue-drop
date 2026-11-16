package com.queuedrop.queue.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.queuedrop.queue.application.dto.QueueMetaRegisterCommand;
import com.queuedrop.queue.application.service.fake.FakeWaitingQueueRepository;
import com.queuedrop.queue.domain.model.QueueMeta;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueueMetaRegisterServiceTest {

    private static final Long DROP_ID = 10L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");

    private FakeWaitingQueueRepository queueRepository;
    private QueueMetaRegisterService service;

    @BeforeEach
    void setUp() {
        queueRepository = new FakeWaitingQueueRepository();
        service = new QueueMetaRegisterService(queueRepository);
    }

    @Test
    @DisplayName("[T3-26] 정상 명령이면 meta 를 1회 저장하고 활성 드로우로 등록된다")
    void registersValidMeta() {
        // Given
        QueueMetaRegisterCommand cmd = new QueueMetaRegisterCommand(DROP_ID, OPEN_AT, CLOSE_AT);

        // When
        service.register(cmd);

        // Then
        assertThat(queueRepository.saveMetaCount()).isEqualTo(1);
        assertThat(queueRepository.storedMeta(DROP_ID)).contains(new QueueMeta(DROP_ID, OPEN_AT, CLOSE_AT));
        assertThat(queueRepository.isActive(DROP_ID)).isTrue();
    }

    @Test
    @DisplayName("[T3-26] µs 시각 명령은 ms 로 내림된 meta 로 저장된다")
    void storesTruncatedMeta() {
        // Given
        QueueMetaRegisterCommand cmd = new QueueMetaRegisterCommand(DROP_ID,
                Instant.parse("2026-11-16T10:00:00.000999Z"), Instant.parse("2026-11-16T12:00:00.000999Z"));

        // When
        service.register(cmd);

        // Then
        QueueMeta stored = queueRepository.storedMeta(DROP_ID).orElseThrow();
        assertThat(stored.openAt()).isEqualTo(OPEN_AT);
        assertThat(stored.closeAt()).isEqualTo(CLOSE_AT);
    }

    static Stream<Arguments> invalidCommands() {
        return Stream.of(
                Arguments.of("openAt == closeAt", new QueueMetaRegisterCommand(DROP_ID, OPEN_AT, OPEN_AT)),
                Arguments.of("openAt > closeAt", new QueueMetaRegisterCommand(DROP_ID, CLOSE_AT, OPEN_AT)),
                Arguments.of("ms 내림 후 같아짐", new QueueMetaRegisterCommand(DROP_ID,
                        Instant.parse("2026-11-16T10:00:00.0001Z"), Instant.parse("2026-11-16T10:00:00.0009Z"))),
                Arguments.of("dropId null", new QueueMetaRegisterCommand(null, OPEN_AT, CLOSE_AT)),
                Arguments.of("openAt null", new QueueMetaRegisterCommand(DROP_ID, null, CLOSE_AT)),
                Arguments.of("closeAt null", new QueueMetaRegisterCommand(DROP_ID, OPEN_AT, null)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCommands")
    @DisplayName("[T3-26] 불변식을 어긴 명령은 예외 없이 저장하지 않는다")
    void skipsInvalidCommand(String caseName, QueueMetaRegisterCommand cmd) {
        // When / Then
        assertThatCode(() -> service.register(cmd)).doesNotThrowAnyException();
        assertThat(queueRepository.saveMetaCount()).isZero();
        assertThat(queueRepository.storedMeta(DROP_ID)).isEmpty();
    }
}
