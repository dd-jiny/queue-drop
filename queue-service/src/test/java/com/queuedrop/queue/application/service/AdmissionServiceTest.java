package com.queuedrop.queue.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.queue.application.config.AdmissionProperties;
import com.queuedrop.queue.application.service.fake.FakeSchedulerGate;
import com.queuedrop.queue.application.service.fake.FakeWaitingQueueRepository;
import com.queuedrop.queue.application.service.fake.FakeWaitingQueueRepository.AdmitCall;
import com.queuedrop.queue.domain.model.QueueMeta;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AdmissionServiceTest {

    /** 현재 시각 10:30 */
    private static final Instant NOW = Instant.parse("2026-11-16T10:30:00Z");

    private static final long INTERVAL_MS = 1_000;
    private static final int BATCH_SIZE = 2;
    private static final long TOKEN_TTL_SECONDS = 300;

    /** 오픈된 드로우 (10:00 ~ 12:00) */
    private static final Long OPEN_DROP = 1L;
    /** 오픈 전 드로우 (11:00 ~ 13:00) */
    private static final Long NOT_OPEN_DROP = 2L;
    /** 또 하나의 오픈된 드로우 (10:00 ~ 11:00) */
    private static final Long OTHER_OPEN_DROP = 3L;
    /** 이미 마감된 드로우 (09:00 ~ 10:30, closeAt == now) */
    private static final Long CLOSED_DROP = 4L;

    private FakeWaitingQueueRepository queueRepository;
    private FakeSchedulerGate schedulerGate;
    private AdmissionService admissionService;

    @BeforeEach
    void setUp() {
        queueRepository = new FakeWaitingQueueRepository();
        schedulerGate = new FakeSchedulerGate();
        admissionService = new AdmissionService(queueRepository, schedulerGate,
                new AdmissionProperties(INTERVAL_MS, BATCH_SIZE, TOKEN_TTL_SECONDS),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static QueueMeta meta(Long dropId, String openAt, String closeAt) {
        return new QueueMeta(dropId, Instant.parse(openAt), Instant.parse(closeAt));
    }

    @Test
    @DisplayName("[T3-10] 게이트를 통과하지 못하면 0 을 반환하고 저장소를 전혀 호출하지 않는다")
    void gateClosedDoesNothing() {
        // Given
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));
        queueRepository.givenWaiting(OPEN_DROP, 100L, 101L);
        schedulerGate.close();

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isZero();
        assertThat(queueRepository.invocationCount()).isZero();
        assertThat(queueRepository.waitingUsers(OPEN_DROP)).containsExactly(100L, 101L);
        assertThat(queueRepository.tokenHolders(OPEN_DROP)).isEmpty();
    }

    @Test
    @DisplayName("[T3-10] 게이트는 이름 \"admission\", window = gateWindowMs(interval 1000 → 900ms) 로 요청한다 (E-31: 지터로 주기를 건너뛰지 않도록 주기보다 짧게)")
    void gateRequestedWithNameAndWindow() {
        // When
        admissionService.admit();

        // Then
        assertThat(schedulerGate.requests())
                .containsExactly(new FakeSchedulerGate.Pass("admission", Duration.ofMillis(900)));
    }

    @Test
    @DisplayName("[T3-11] 활성 드로우 2개 중 1개가 오픈 전이면 오픈된 드로우만 입장 처리한다")
    void admitsOnlyOpenDrop() {
        // Given
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));
        queueRepository.givenMeta(meta(NOT_OPEN_DROP, "2026-11-16T11:00:00Z", "2026-11-16T13:00:00Z"));
        queueRepository.givenWaiting(OPEN_DROP, 100L, 101L, 102L);
        queueRepository.givenWaiting(NOT_OPEN_DROP, 200L, 201L);

        // When
        int admitted = admissionService.admit();

        // Then — 오픈된 드로우는 앞에서 batchSize 명만 입장, 오픈 전 드로우는 그대로
        assertThat(admitted).isEqualTo(2);
        assertThat(queueRepository.tokenHolders(OPEN_DROP)).containsExactlyInAnyOrder(100L, 101L);
        assertThat(queueRepository.waitingUsers(OPEN_DROP)).containsExactly(102L);
        assertThat(queueRepository.tokenHolders(NOT_OPEN_DROP)).isEmpty();
        assertThat(queueRepository.waitingUsers(NOT_OPEN_DROP)).containsExactly(200L, 201L);
        assertThat(queueRepository.admitCalls()).containsExactly(
                new AdmitCall(OPEN_DROP, BATCH_SIZE, Duration.ofSeconds(TOKEN_TTL_SECONDS), NOW));
    }

    @Test
    @DisplayName("[T3-11] 오픈된 드로우가 여럿이면 드로우마다 batchSize 를 적용하고 입장 인원 합을 반환한다")
    void sumsAdmittedAcrossOpenDrops() {
        // Given — 드로우1: 3명 대기(2명 입장), 드로우3: 1명 대기(1명 입장)
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));
        queueRepository.givenMeta(meta(OTHER_OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T11:00:00Z"));
        queueRepository.givenWaiting(OPEN_DROP, 100L, 101L, 102L);
        queueRepository.givenWaiting(OTHER_OPEN_DROP, 300L);

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isEqualTo(3);
        assertThat(queueRepository.tokenHolders(OPEN_DROP)).containsExactlyInAnyOrder(100L, 101L);
        assertThat(queueRepository.tokenHolders(OTHER_OPEN_DROP)).containsExactly(300L);
    }

    @Test
    @DisplayName("[T3-11] 한 드로우의 admit 이 예외를 던져도 다음 드로우를 계속 처리하고, 성공한 드로우의 입장 합을 반환한다")
    void admitFailureIsIsolatedPerDrop() {
        // Given — 실패하는 드로우를 먼저 등록해 루프 앞쪽에 둔다
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));
        queueRepository.givenMeta(meta(OTHER_OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T11:00:00Z"));
        queueRepository.givenWaiting(OPEN_DROP, 100L);
        queueRepository.givenWaiting(OTHER_OPEN_DROP, 300L, 301L);
        queueRepository.failAdmitFor(OPEN_DROP);

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isEqualTo(2);
        assertThat(queueRepository.tokenHolders(OTHER_OPEN_DROP)).containsExactlyInAnyOrder(300L, 301L);
        assertThat(queueRepository.waitingUsers(OPEN_DROP)).containsExactly(100L);
    }

    @Test
    @DisplayName("[T3-11] 한 드로우의 findMeta 가 예외를 던져도 다음 드로우를 계속 처리한다")
    void findMetaFailureIsIsolatedPerDrop() {
        // Given
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));
        queueRepository.givenMeta(meta(OTHER_OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T11:00:00Z"));
        queueRepository.givenWaiting(OPEN_DROP, 100L);
        queueRepository.givenWaiting(OTHER_OPEN_DROP, 300L);
        queueRepository.failFindMetaFor(OPEN_DROP);

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isEqualTo(1);
        assertThat(queueRepository.tokenHolders(OTHER_OPEN_DROP)).containsExactly(300L);
        assertThat(queueRepository.tokenHolders(OPEN_DROP)).isEmpty();
    }

    @Test
    @DisplayName("[T3-11] 마감된 드로우(closeAt == now)는 활성 인덱스에서 제거되고 입장 처리하지 않는다")
    void closedDropIsRemovedAndSkipped() {
        // Given
        queueRepository.givenMeta(meta(CLOSED_DROP, "2026-11-16T09:00:00Z", "2026-11-16T10:30:00Z"));
        queueRepository.givenWaiting(CLOSED_DROP, 400L);

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isZero();
        assertThat(queueRepository.isActive(CLOSED_DROP)).isFalse();
        assertThat(queueRepository.tokenHolders(CLOSED_DROP)).isEmpty();
        assertThat(queueRepository.admitCalls()).isEmpty();
    }

    @Test
    @DisplayName("[T3-11] 오픈된 드로우의 대기열이 비어 있으면 0 을 반환한다")
    void emptyQueueAdmitsNobody() {
        // Given
        queueRepository.givenMeta(meta(OPEN_DROP, "2026-11-16T10:00:00Z", "2026-11-16T12:00:00Z"));

        // When
        int admitted = admissionService.admit();

        // Then
        assertThat(admitted).isZero();
        assertThat(queueRepository.tokenHolders(OPEN_DROP)).isEmpty();
    }
}
