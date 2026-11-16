package com.queuedrop.queue.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.queue.application.dto.QueueEnterCommand;
import com.queuedrop.queue.application.dto.QueueStatusQuery;
import com.queuedrop.queue.application.dto.QueueStatusResult;
import com.queuedrop.queue.application.exception.QueueErrorCode;
import com.queuedrop.queue.application.service.fake.FakeRemainingCapacityReader;
import com.queuedrop.queue.application.service.fake.FakeWaitingQueueRepository;
import com.queuedrop.queue.domain.model.QueueMeta;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueueServiceTest {

    private static final Long DROP_ID = 10L;
    private static final Long USER_ID = 7L;
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");
    /** 오픈 중 시각 */
    private static final Instant NOW = Instant.parse("2026-11-16T10:30:00Z");

    private FakeWaitingQueueRepository queueRepository;
    private FakeRemainingCapacityReader remainingReader;

    @BeforeEach
    void setUp() {
        queueRepository = new FakeWaitingQueueRepository();
        remainingReader = new FakeRemainingCapacityReader();
    }

    private QueueService serviceAt(Instant now) {
        return new QueueService(queueRepository, remainingReader, new PollIntervalPolicy(),
                Clock.fixed(now, ZoneOffset.UTC));
    }

    private void givenOpenDrop() {
        queueRepository.givenMeta(new QueueMeta(DROP_ID, OPEN_AT, CLOSE_AT));
    }

    private static QueueEnterCommand enter(Long userId) {
        return new QueueEnterCommand(DROP_ID, userId);
    }

    private static QueueStatusQuery status(Long userId) {
        return new QueueStatusQuery(DROP_ID, userId);
    }

    @Nested
    @DisplayName("enter — 진입")
    class Enter {

        @Test
        @DisplayName("[T3-03] meta 가 없으면 DROP_NOT_FOUND")
        void dropNotFound() {
            // Given — meta 없음
            QueueService service = serviceAt(NOW);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.DROP_NOT_FOUND));
            assertThat(queueRepository.waitingUsers(DROP_ID)).isEmpty();
        }

        @Test
        @DisplayName("[T3-04] 오픈 직전(09:59:59.999)이면 NOT_OPEN, 대기열에 들어가지 않는다")
        void notOpen() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            QueueService service = serviceAt(OPEN_AT.minusMillis(1));

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.NOT_OPEN));
            assertThat(queueRepository.waitingUsers(DROP_ID)).isEmpty();
        }

        @Test
        @DisplayName("[T3-04] 오픈 정각(10:00)이면 진입된다")
        void openAtExactOpen() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            QueueService service = serviceAt(OPEN_AT);

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result.state()).isEqualTo("WAITING");
            assertThat(queueRepository.waitingUsers(DROP_ID)).containsExactly(USER_ID);
        }

        @Test
        @DisplayName("[T3-04] 마감 정각(12:00)이면 CLOSED, 대기열에 들어가지 않는다")
        void closed() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            QueueService service = serviceAt(CLOSE_AT);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.CLOSED));
            assertThat(queueRepository.waitingUsers(DROP_ID)).isEmpty();
        }

        @Test
        @DisplayName("[T3-04] 마감 직전(11:59:59.999)이면 진입된다")
        void openJustBeforeClose() {
            // Given
            givenOpenDrop();
            QueueService service = serviceAt(CLOSE_AT.minusMillis(1));

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result.state()).isEqualTo("WAITING");
        }

        @Test
        @DisplayName("[T3-05] 잔여 정원 0 이면 SOLD_OUT, 대기열에 들어가지 않는다")
        void soldOut() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 0);
            QueueService service = serviceAt(NOW);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.SOLD_OUT));
            assertThat(queueRepository.waitingUsers(DROP_ID)).isEmpty();
        }

        @Test
        @DisplayName("[T3-05] 잔여 정원이 음수여도 SOLD_OUT")
        void soldOutNegative() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, -1);
            QueueService service = serviceAt(NOW);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.SOLD_OUT));
        }

        @Test
        @DisplayName("[T3-05] 잔여 정원 1 이면 품절이 아니므로 진입된다 (경계)")
        void remainingOneAllowsEnter() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 1);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result.state()).isEqualTo("WAITING");
            assertThat(queueRepository.waitingUsers(DROP_ID)).containsExactly(USER_ID);
        }

        @Test
        @DisplayName("[T3-04] 오픈 전이면서 잔여 0 이면 NOT_OPEN 이 SOLD_OUT 보다 먼저다")
        void notOpenCheckedBeforeSoldOut() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 0);
            QueueService service = serviceAt(OPEN_AT.minusMillis(1));

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.NOT_OPEN));
        }

        @Test
        @DisplayName("[T3-04] 마감 후이면서 잔여 0 이면 CLOSED 가 SOLD_OUT 보다 먼저다")
        void closedCheckedBeforeSoldOut() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 0);
            QueueService service = serviceAt(CLOSE_AT);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.enter(enter(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.CLOSED));
        }

        @Test
        @DisplayName("[T3-06] 잔여 정원 키가 없으면 품절이 아니므로 진입을 허용한다 (WAITING, 앞 0명, 1초 후 폴링)")
        void remainingKeyMissingAllowsEnter() {
            // Given — remainingReader 에 값 없음
            givenOpenDrop();
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("WAITING", 0L, 1_000));
            assertThat(queueRepository.waitingUsers(DROP_ID)).containsExactly(USER_ID);
        }

        @Test
        @DisplayName("[T3-07] 같은 사용자가 두 번 진입해도 aheadCount 가 같고 대기열에 한 번만 선다 (멱등)")
        void enterIsIdempotent() {
            // Given — 앞에 1명 대기
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            queueRepository.givenWaiting(DROP_ID, 1L);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult first = service.enter(enter(USER_ID));
            QueueStatusResult second = service.enter(enter(USER_ID));

            // Then
            assertThat(first).isEqualTo(new QueueStatusResult("WAITING", 1L, 1_000));
            assertThat(second).isEqualTo(first);
            assertThat(queueRepository.waitingUsers(DROP_ID)).containsExactly(1L, USER_ID);
        }

        @Test
        @DisplayName("[T3-07] 앞 인원이 100명이면 nextPollAfterMs 가 3초다 (폴링 정책 연동)")
        void enterUsesPollPolicy() {
            // Given — 앞에 100명 대기
            givenOpenDrop();
            queueRepository.givenWaiting(DROP_ID, LongStream.rangeClosed(1_000, 1_099).boxed().toArray(Long[]::new));
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("WAITING", 100L, 3_000));
        }

        @Test
        @DisplayName("[T3-08] 이미 입장 토큰을 가진 사용자가 진입하면 ADMITTED (aheadCount null, 폴링 0), 다시 줄 서지 않는다")
        void enterWhenAlreadyAdmitted() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            queueRepository.givenToken(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.enter(enter(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("ADMITTED", null, 0));
            assertThat(queueRepository.waitingUsers(DROP_ID)).isEmpty();
        }
    }

    @Nested
    @DisplayName("status — 상태 조회 (SOLD_OUT > ADMITTED > WAITING > NOT_IN_QUEUE)")
    class Status {

        @Test
        @DisplayName("[T3-03] meta 가 없으면 DROP_NOT_FOUND")
        void dropNotFound() {
            // Given — meta 없음
            QueueService service = serviceAt(NOW);

            // When / Then
            assertThatExceptionOfType(ApplicationException.class)
                    .isThrownBy(() -> service.status(status(USER_ID)))
                    .satisfies(e -> assertThat(e.errorCode()).isEqualTo(QueueErrorCode.DROP_NOT_FOUND));
        }

        @Test
        @DisplayName("[T3-05] 잔여 정원 0 이면 토큰 보유자도 SOLD_OUT")
        void soldOutBeatsAdmitted() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 0);
            queueRepository.givenToken(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("SOLD_OUT", null, 0));
        }

        @Test
        @DisplayName("[T3-05] 잔여 정원 0 이면 대기 중인 사용자도 SOLD_OUT")
        void soldOutBeatsWaiting() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 0);
            queueRepository.givenWaiting(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("SOLD_OUT", null, 0));
        }

        @Test
        @DisplayName("[T3-08] 토큰 보유 → ADMITTED (aheadCount null, 폴링 0)")
        void admitted() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 1);
            queueRepository.givenToken(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("ADMITTED", null, 0));
        }

        @Test
        @DisplayName("[T3-08] 대기 중 → WAITING (앞 인원 수, 폴링 간격)")
        void waiting() {
            // Given — 앞에 2명
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            queueRepository.givenWaiting(DROP_ID, 1L, 2L, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("WAITING", 2L, 1_000));
        }

        @Test
        @DisplayName("[T3-08] 토큰도 대기도 없음 → NOT_IN_QUEUE (aheadCount null, 폴링 0)")
        void notInQueue() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("NOT_IN_QUEUE", null, 0));
        }

        @Test
        @DisplayName("[T3-08] 상태 조회 중 입장 처리가 끼어들어도 NOT_IN_QUEUE 로 잘못 보이지 않는다 (rank 를 hasToken 보다 먼저 조회)")
        void admissionRaceIsNotReportedAsNotInQueue() {
            // Given — 대기 중인 사용자. 첫 조회 직후 입장 처리가 끼어든다
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            queueRepository.givenWaiting(DROP_ID, USER_ID);
            queueRepository.admitRightAfterFirstRead(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then — rank 를 먼저 읽었으므로 WAITING (다음 폴링에서 ADMITTED). hasToken 을 먼저 읽으면 NOT_IN_QUEUE 가 된다
            assertThat(result.state()).isEqualTo("WAITING");
            assertThat(queueRepository.readCalls()).first().isEqualTo("rank");
        }

        @Test
        @DisplayName("[T3-08] 대기 중이면 hasToken 을 조회하지 않고, 대기에 없을 때만 hasToken 을 조회한다")
        void hasTokenOnlyWhenNotWaiting() {
            // Given
            givenOpenDrop();
            remainingReader.set(DROP_ID, 100);
            queueRepository.givenWaiting(DROP_ID, USER_ID);
            queueRepository.givenToken(DROP_ID, 8L);
            QueueService service = serviceAt(NOW);

            // When
            service.status(status(USER_ID));
            QueueStatusResult admitted = service.status(status(8L));

            // Then
            assertThat(admitted.state()).isEqualTo("ADMITTED");
            assertThat(queueRepository.readCalls()).containsExactly("rank", "rank", "hasToken");
        }

        @Test
        @DisplayName("[T3-06] 잔여 정원 키가 없으면 품절로 보지 않고 대기 상태를 그대로 판정한다")
        void remainingKeyMissingIsNotSoldOut() {
            // Given — remainingReader 에 값 없음
            givenOpenDrop();
            queueRepository.givenWaiting(DROP_ID, USER_ID);
            QueueService service = serviceAt(NOW);

            // When
            QueueStatusResult result = service.status(status(USER_ID));

            // Then
            assertThat(result).isEqualTo(new QueueStatusResult("WAITING", 0L, 1_000));
        }
    }
}
