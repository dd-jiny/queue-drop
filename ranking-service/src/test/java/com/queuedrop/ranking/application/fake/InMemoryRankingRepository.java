package com.queuedrop.ranking.application.fake;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.RankEntry;
import com.queuedrop.ranking.domain.model.RankingBoard;
import com.queuedrop.ranking.domain.model.WeekKey;
import com.queuedrop.ranking.domain.repository.RankingRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Redis 랭킹 저장소(rank.lua + ZSet)의 인메모리 대역 (단계 문서 3.2·3.3·3.5).
 * <ul>
 *   <li>incrementOnce: (dropId, userId) 를 처음 보면 인기·주간(week)·누적 보드에 1점씩 더하고 true</li>
 *   <li>top: 점수 내림차순, 동점은 member 문자열 사전 역순 (ZREVRANGE 와 같음). rank = offset + index + 1</li>
 *   <li>USER_WEEKLY 에 week 가 null 이면 NPE (RankingKeys.board 와 같음)</li>
 *   <li>fail*(): 이후 해당 호출이 모두 RankingStoreUnavailableException. 호출 기록은 실패해도 남는다</li>
 * </ul>
 */
public class InMemoryRankingRepository implements RankingRepository {

    public record IncrementCall(long dropId, long userId, WeekKey week) {}

    public record TopCall(RankingBoard board, WeekKey week, long offset, int count) {}

    public record RankOfCall(RankingBoard board, WeekKey week, long memberId) {}

    private final Map<String, Map<Long, Long>> boards = new HashMap<>();
    private final Set<String> dedupe = new HashSet<>();

    private final List<IncrementCall> incrementCalls = new ArrayList<>();
    private final List<TopCall> topCalls = new ArrayList<>();
    private final List<RankOfCall> rankOfCalls = new ArrayList<>();
    private final List<List<Long>> removeDropsCalls = new ArrayList<>();

    /** top 호출 번호(1부터) → 그 호출 결과 앞에 끼워 넣을 (memberId, score). 배치 사이 보드 변경 재현용 */
    private final Map<Integer, List<long[]>> topInjections = new HashMap<>();

    private boolean failIncrement;
    private boolean failTop;
    private boolean failRankOf;
    private boolean failRemoveDrops;
    private RankingStoreUnavailableException lastFailure;

    // ---------------------------------------------------------------- 포트 구현

    @Override
    public boolean incrementOnce(long dropId, long userId, WeekKey week) {
        incrementCalls.add(new IncrementCall(dropId, userId, week));
        Objects.requireNonNull(week, "week 필수");
        if (failIncrement) {
            throw unavailable("fake incrementOnce 실패");
        }
        if (!dedupe.add(dropId + ":" + userId)) {
            return false;
        }
        board(RankingBoard.DROP_POPULAR, null).merge(dropId, 1L, Long::sum);
        board(RankingBoard.USER_WEEKLY, week).merge(userId, 1L, Long::sum);
        board(RankingBoard.USER_ALL, null).merge(userId, 1L, Long::sum);
        return true;
    }

    @Override
    public List<RankEntry> top(RankingBoard board, WeekKey week, long offset, int count) {
        topCalls.add(new TopCall(board, week, offset, count));
        String key = key(board, week);
        if (failTop) {
            throw unavailable("fake top 실패");
        }
        List<Map.Entry<Long, Long>> sorted = sorted(key);
        List<long[]> members = new ArrayList<>(topInjections.getOrDefault(topCalls.size(), List.of()));
        for (long i = offset; i < Math.min(sorted.size(), offset + count); i++) {
            Map.Entry<Long, Long> e = sorted.get((int) i);
            members.add(new long[] {e.getKey(), e.getValue()});
        }
        List<RankEntry> result = new ArrayList<>();
        for (int i = 0; i < Math.min(members.size(), count); i++) {
            result.add(new RankEntry(offset + i + 1, members.get(i)[0], members.get(i)[1]));
        }
        return result;
    }

    @Override
    public Optional<RankEntry> rankOf(RankingBoard board, WeekKey week, long memberId) {
        rankOfCalls.add(new RankOfCall(board, week, memberId));
        String key = key(board, week);
        if (failRankOf) {
            throw unavailable("fake rankOf 실패");
        }
        List<Map.Entry<Long, Long>> sorted = sorted(key);
        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).getKey() == memberId) {
                return Optional.of(new RankEntry(i + 1L, memberId, sorted.get(i).getValue()));
            }
        }
        return Optional.empty();
    }

    @Override
    public void removeDrops(Collection<Long> dropIds) {
        removeDropsCalls.add(List.copyOf(dropIds));
        if (failRemoveDrops) {
            throw unavailable("fake removeDrops 실패");
        }
        Map<Long, Long> popular = board(RankingBoard.DROP_POPULAR, null);
        dropIds.forEach(popular::remove);
    }

    private List<Map.Entry<Long, Long>> sorted(String key) {
        Comparator<Map.Entry<Long, Long>> byScoreDesc =
                Comparator.comparing(Map.Entry<Long, Long>::getValue, Comparator.reverseOrder());
        Comparator<Map.Entry<Long, Long>> byMemberStringDesc =
                Comparator.comparing(e -> String.valueOf(e.getKey()), Comparator.reverseOrder());
        return boards.getOrDefault(key, Map.of()).entrySet().stream()
                .sorted(byScoreDesc.thenComparing(byMemberStringDesc))
                .map(e -> Map.entry(e.getKey(), e.getValue()))
                .toList();
    }

    private Map<Long, Long> board(RankingBoard board, WeekKey week) {
        return boards.computeIfAbsent(key(board, week), k -> new HashMap<>());
    }

    private static String key(RankingBoard board, WeekKey week) {
        return switch (board) {
            case DROP_POPULAR -> "popular";
            case USER_ALL -> "all";
            case USER_WEEKLY -> "weekly:" + Objects.requireNonNull(week, "USER_WEEKLY 는 week 필수").value();
        };
    }

    private RankingStoreUnavailableException unavailable(String message) {
        lastFailure = new RankingStoreUnavailableException(message, new RuntimeException("simulated redis failure"));
        return lastFailure;
    }

    // ---------------------------------------------------------------- 테스트 조작

    /** 보드에 점수를 직접 넣는다 (dedupe 와 무관). week 는 USER_WEEKLY 일 때만 */
    public void givenScore(RankingBoard board, WeekKey week, long memberId, long score) {
        board(board, week).put(memberId, score);
    }

    /**
     * callNumber 번째(1부터) top 호출 결과 맨 앞에 (memberId, score) 를 끼워 넣는다 (결과는 count 로 자른다).
     * 배치 사이 ZINCRBY 로 순서가 바뀌어 이전 배치의 id 가 다음 배치에 다시 나오는 상황을 재현한다.
     */
    public void injectOnTopCall(int callNumber, long memberId, long score) {
        topInjections.computeIfAbsent(callNumber, k -> new ArrayList<>()).add(new long[] {memberId, score});
    }

    public void failIncrement() {
        this.failIncrement = true;
    }

    public void failTop() {
        this.failTop = true;
    }

    public void failRankOf() {
        this.failRankOf = true;
    }

    public void failRemoveDrops() {
        this.failRemoveDrops = true;
    }

    // ---------------------------------------------------------------- 상태 조회 (호출 기록 없음)

    /** 보드의 점수 (member → score) */
    public Map<Long, Long> scores(RankingBoard board, WeekKey week) {
        return Map.copyOf(boards.getOrDefault(key(board, week), Map.of()));
    }

    public List<IncrementCall> incrementCalls() {
        return List.copyOf(incrementCalls);
    }

    public List<TopCall> topCalls() {
        return List.copyOf(topCalls);
    }

    public List<RankOfCall> rankOfCalls() {
        return List.copyOf(rankOfCalls);
    }

    public List<List<Long>> removeDropsCalls() {
        return List.copyOf(removeDropsCalls);
    }

    public RankingStoreUnavailableException lastFailure() {
        return lastFailure;
    }
}
