package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.model.EntryStatus;
import com.queuedrop.entry.domain.model.SnapshotStatus;
import com.queuedrop.entry.domain.repository.EntryRepository;
import com.queuedrop.entry.domain.repository.EntrySaveOutcome;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
class EntryRepositoryImpl implements EntryRepository {

    /** IN 절 파라미터 수 제한(PostgreSQL 32767) 대비 분할 크기. */
    static final int WINNER_CHUNK_SIZE = 1_000;

    private final EntryJpaRepository jpa;
    private final DropSnapshotJpaRepository snapshotJpa;

    /**
     * 추첨이 끝난(DRAWN) 드로우면 LOST 로 저장한다 (E-48).
     * DRAWN 판정을 도메인이 아닌 여기서 하는 이유: 스냅샷 행 FOR SHARE 로 추첨 트랜잭션의 첫 UPDATE 와
     * 같은 행을 잠가, 상태 확인과 INSERT 를 추첨과 원자적으로 직렬화해야 하기 때문이다.
     * 스냅샷이 없으면(드로우 정보 수신 전) 추첨 전이 확실하므로 그대로 APPLIED.
     */
    @Override
    @Transactional
    public EntrySaveOutcome saveIfAbsent(Entry entry) {
        boolean drawn = snapshotJpa.findStatusForShare(entry.getDropId())
                .map(status -> SnapshotStatus.DRAWN.name().equals(status))
                .orElse(false);
        String status = drawn ? EntryStatus.LOST.name() : entry.getStatus().name();
        int inserted = jpa.insertIfAbsent(entry.getDropId(), entry.getUserId(), entry.getEntrySeq(),
                status, entry.getAppliedAt());
        if (inserted == 0) {
            return EntrySaveOutcome.ALREADY_EXISTS;
        }
        return drawn ? EntrySaveOutcome.SAVED_AS_LOST : EntrySaveOutcome.SAVED;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Entry> findByDropIdAndUserId(long dropId, long userId) {
        return jpa.findByDropIdAndUserId(dropId, userId).map(EntryMapper::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Entry> findAllByUserId(long userId) {
        return jpa.findAllByUserIdOrderByAppliedAtDesc(userId).stream().map(EntryMapper::toDomain).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public long countByDropId(long dropId) {
        return jpa.countByDropId(dropId);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Long> findUserIdsOrderBySeq(long dropId) {
        return jpa.findUserIdsOrderBySeq(dropId);
    }

    /** 당첨자를 분할해 WON 으로 바꾼 뒤 남은 APPLIED 를 모두 LOST 로. 빈 집합이면 IN 절을 만들지 않는다. */
    @Override
    @Transactional
    public void markResults(long dropId, Set<Long> winnerUserIds) {
        List<Long> winners = new ArrayList<>(winnerUserIds);
        for (int from = 0; from < winners.size(); from += WINNER_CHUNK_SIZE) {
            jpa.markWon(dropId, winners.subList(from, Math.min(from + WINNER_CHUNK_SIZE, winners.size())));
        }
        jpa.markRemainingLost(dropId);
    }
}
