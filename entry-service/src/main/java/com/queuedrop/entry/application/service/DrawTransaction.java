package com.queuedrop.entry.application.service;

import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.dto.DrawOutcome;
import com.queuedrop.entry.application.port.SeedGenerator;
import com.queuedrop.entry.domain.model.DrawResult;
import com.queuedrop.entry.domain.model.DropSnapshot;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import com.queuedrop.entry.domain.repository.DropSnapshotRepository;
import com.queuedrop.entry.domain.repository.EntryRepository;
import com.queuedrop.entry.domain.service.Drawer;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 추첨 단일 트랜잭션. DrawService 의 자기 호출로는 프록시가 적용되지 않아 별도 빈으로 둔다.
 * DRAWN 전이를 첫 문장으로 두어 행 잠금을 잡고, 어느 단계에서 실패해도 롤백되어 CLOSING 으로 돌아간다.
 * Redis 호출·발행은 넣지 않는다.
 */
@Component
@RequiredArgsConstructor
public class DrawTransaction {

    private final DropSnapshotRepository snapshots;
    private final EntryRepository entries;
    private final DrawResultRepository results;
    private final SeedGenerator seedGenerator;
    private final Drawer drawer = new Drawer();

    @Transactional
    public DrawOutcome execute(Long dropId, Instant now) {
        // ① CLOSING → DRAWN 조건부 UPDATE: 행 잠금 획득. 다른 인스턴스는 여기서 대기 후 false
        if (!snapshots.markDrawnIfClosing(dropId)) {
            return DrawOutcome.skipped();
        }
        DropSnapshot snapshot = snapshots.findById(dropId).orElseThrow();

        // ② 결정적 순서로 응모자 조회
        List<Long> applicants = entries.findUserIdsOrderBySeq(dropId);

        // ③ 시드 셔플
        long seed = seedGenerator.next();
        Set<Long> winners = drawer.draw(applicants, snapshot.getWinnerCount(), seed);

        // ④ 벌크 상태 반영
        entries.markResults(dropId, winners);

        // ⑤ 추첨 기록 (drop_id UK → 중복 추첨 최종 방어선)
        DrawResult result = results.save(DrawResult.of(dropId, seed, applicants.size(), winners.size(), now));

        return new DrawOutcome(true, new DrawCompletedPayload(
                dropId, result.getApplicantCount(), result.getWinnerCount(), now));
    }
}
