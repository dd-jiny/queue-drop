package com.queuedrop.ranking.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.ranking.application.dto.DropRankResult;
import com.queuedrop.ranking.application.dto.MyRankResult;
import com.queuedrop.ranking.application.dto.UserRankResult;
import com.queuedrop.ranking.application.dto.WeeklyRankingResult;
import com.queuedrop.ranking.application.exception.RankingErrorCode;
import com.queuedrop.ranking.domain.exception.InvalidWeekKeyException;
import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.DropWindow;
import com.queuedrop.ranking.domain.model.RankEntry;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import com.queuedrop.ranking.domain.repository.DisplayNameRepository;
import com.queuedrop.ranking.domain.repository.DropWindowRepository;
import com.queuedrop.ranking.domain.repository.RankingRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 조회 (읽기 경로) */
@Slf4j
@Service
@RequiredArgsConstructor
public class RankingQueryService {

    private static final int MAX_SIZE = 100;
    static final int SCAN_BATCH = 50;                   // (구현 결정)
    static final int SCAN_LIMIT = 500;                  // 진행 중 필터 시 최대 탐색 수 (구현 결정)

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;
    private final Clock clock;                          // java.time.Clock Bean (infrastructure/config/ClockConfig)

    /** 진행 중 드로우만 응모 수 순으로 (확정, 2026-11-17 사용자 결정). 탐색 중 본 종료 드로우는 lazy 제거 */
    public List<DropRankResult> topDrops(int size) {
        validateSize(size);
        Instant now = clock.instant();
        List<RankEntry> picked = new ArrayList<>();
        List<Long> closed = new ArrayList<>();
        Set<Long> seen = new HashSet<>();               // 배치 사이 보드 변경 시 같은 id 중복 방지
        store(() -> {
            for (long offset = 0; picked.size() < size && offset < SCAN_LIMIT; offset += SCAN_BATCH) {
                List<RankEntry> batch = rankingRepository.top(RankingBoard.DROP_POPULAR, null, offset, SCAN_BATCH);
                Map<Long, DropWindow> windows = dropWindowRepository.findAll(ids(batch));
                for (RankEntry e : batch) {
                    if (!seen.add(e.memberId())) continue;
                    DropWindow w = windows.getOrDefault(e.memberId(), new DropWindow(e.memberId(), null));
                    if (w.isClosed(now)) closed.add(e.memberId());
                    else if (picked.size() < size) picked.add(e);
                }
                if (batch.size() < SCAN_BATCH) break;    // 마지막 배치 — 다음 조회 생략
            }
        });
        removeClosed(closed);                            // 탐색이 끝난 뒤에 지운다 (탐색 중 지우면 offset 이 밀린다)
        Map<Long, String> titles = store(() -> displayNameRepository.findDropTitles(ids(picked)));
        // 필터 후 순번을 1부터 다시 매긴다 (구현 결정)
        return IntStream.range(0, picked.size())
                .mapToObj(i -> {
                    long dropId = picked.get(i).memberId();
                    return new DropRankResult(i + 1, dropId,
                            titles.getOrDefault(dropId, "drop-" + dropId), picked.get(i).score());
                })
                .toList();
    }

    /** week 가 null 이면 이번 주 */
    public WeeklyRankingResult weeklyUsers(String week, int size) {
        validateSize(size);
        WeekKey key = (week == null) ? WeekKey.of(clock.instant()) : parseWeek(week);
        List<RankEntry> entries = store(() -> rankingRepository.top(RankingBoard.USER_WEEKLY, key, 0, size));
        return new WeeklyRankingResult(key.value(), withNicknames(entries));
    }

    public List<UserRankResult> allUsers(int size) {
        validateSize(size);
        return withNicknames(store(() -> rankingRepository.top(RankingBoard.USER_ALL, null, 0, size)));
    }

    public MyRankResult myRank(long userId) {
        WeekKey thisWeek = WeekKey.of(clock.instant());
        Optional<RankEntry> weekly = store(() -> rankingRepository.rankOf(RankingBoard.USER_WEEKLY, thisWeek, userId));
        Optional<RankEntry> all = store(() -> rankingRepository.rankOf(RankingBoard.USER_ALL, null, userId));
        return new MyRankResult(thisWeek.value(),
                weekly.map(RankEntry::rank).orElse(null), weekly.map(RankEntry::score).orElse(0L),
                all.map(RankEntry::rank).orElse(null), all.map(RankEntry::score).orElse(0L));
    }

    /** lazy 제거는 부가 작업이다 — 실패해도 조회 결과는 돌려준다 (다음 조회에서 다시 시도) */
    private void removeClosed(List<Long> closed) {
        if (closed.isEmpty()) return;
        try {
            rankingRepository.removeDrops(closed);
        } catch (RankingStoreUnavailableException e) {
            log.warn("closed drop lazy removal failed: count={} - {}", closed.size(), e.getMessage());
        }
    }

    /** 닉네임이 없으면 "user-{id}" */
    private List<UserRankResult> withNicknames(List<RankEntry> entries) {
        Map<Long, String> nicknames = store(() -> displayNameRepository.findNicknames(ids(entries)));
        return entries.stream()
                .map(e -> new UserRankResult(e.rank(), e.memberId(),
                        nicknames.getOrDefault(e.memberId(), "user-" + e.memberId()), e.score()))
                .toList();
    }

    private static List<Long> ids(List<RankEntry> entries) {
        return entries.stream().map(RankEntry::memberId).toList();
    }

    /** Redis 장애 → 503 (E-03, timeout 1s) */
    private <T> T store(Supplier<T> call) {
        try {
            return call.get();
        } catch (RankingStoreUnavailableException e) {
            throw new ApplicationException(RankingErrorCode.TEMPORARILY_FAILED, e);
        }
    }

    private void store(Runnable call) {
        store(() -> {
            call.run();
            return null;
        });
    }

    private WeekKey parseWeek(String week) {
        try {
            return WeekKey.parse(week);
        } catch (InvalidWeekKeyException e) {
            throw new ApplicationException(RankingErrorCode.INVALID_WEEK);
        }
    }

    private void validateSize(int size) {
        if (size < 1 || size > MAX_SIZE) throw new ApplicationException(RankingErrorCode.INVALID_SIZE);
    }
}
