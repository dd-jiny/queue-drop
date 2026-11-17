package com.queuedrop.entry.infrastructure.persistence;

import com.queuedrop.entry.domain.model.Entry;
import com.queuedrop.entry.domain.repository.EntryRepository;
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

    @Override
    @Transactional
    public boolean saveIfAbsent(Entry entry) {
        int inserted = jpa.insertIfAbsent(entry.getDropId(), entry.getUserId(), entry.getEntrySeq(),
                entry.getStatus().name(), entry.getAppliedAt());
        return inserted == 1;
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
