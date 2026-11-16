package com.queuedrop.queue.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.queue.application.dto.QueueEnterCommand;
import com.queuedrop.queue.application.dto.QueueStatusQuery;
import com.queuedrop.queue.application.dto.QueueStatusResult;
import com.queuedrop.queue.application.exception.QueueErrorCode;
import com.queuedrop.queue.application.port.RemainingCapacityReader;
import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 대기열 진입·상태 조회. DB 가 없으므로 @Transactional 없음 */
@Service
@RequiredArgsConstructor
public class QueueService {

    private final WaitingQueueRepository queueRepository;
    private final RemainingCapacityReader remainingReader;
    private final PollIntervalPolicy pollPolicy;
    private final Clock clock;

    public QueueStatusResult enter(QueueEnterCommand cmd) {
        QueueMeta meta = findMeta(cmd.dropId());
        Instant now = clock.instant();
        if (meta.isBeforeOpen(now)) throw new ApplicationException(QueueErrorCode.NOT_OPEN);
        if (meta.isClosed(now)) throw new ApplicationException(QueueErrorCode.CLOSED);
        if (isSoldOut(cmd.dropId())) throw new ApplicationException(QueueErrorCode.SOLD_OUT);

        EnqueueResult r = queueRepository.enqueue(cmd.dropId(), cmd.userId());
        return r.admitted() ? QueueStatusResult.admitted() : waiting(r.rank());
    }

    /** 우선순위 SOLD_OUT > ADMITTED > WAITING > NOT_IN_QUEUE */
    public QueueStatusResult status(QueueStatusQuery q) {
        findMeta(q.dropId());
        if (isSoldOut(q.dropId())) return QueueStatusResult.soldOut();
        if (queueRepository.hasToken(q.dropId(), q.userId())) return QueueStatusResult.admitted();
        OptionalLong rank = queueRepository.rank(q.dropId(), q.userId());
        return rank.isPresent() ? waiting(rank.getAsLong()) : QueueStatusResult.notInQueue();
    }

    private QueueMeta findMeta(Long dropId) {
        return queueRepository.findMeta(dropId)
                .orElseThrow(() -> new ApplicationException(QueueErrorCode.DROP_NOT_FOUND));
    }

    private QueueStatusResult waiting(long aheadCount) {
        return QueueStatusResult.waiting(aheadCount, pollPolicy.nextPollAfterMs(aheadCount));
    }

    /** 키 없음(entry 미초기화)은 품절이 아니다 — 진입 허용, 입장 보류 */
    private boolean isSoldOut(Long dropId) {
        OptionalLong remaining = remainingReader.read(dropId);
        return remaining.isPresent() && remaining.getAsLong() <= 0;
    }
}
