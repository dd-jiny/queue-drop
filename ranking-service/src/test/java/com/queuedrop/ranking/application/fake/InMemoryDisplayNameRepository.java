package com.queuedrop.ranking.application.fake;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.repository.DisplayNameRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** rank:drop:info / rank:user:info Hash 의 인메모리 대역. 없는 id 는 맵에서 제외. fail*() 이후 해당 호출은 모두 실패 */
public class InMemoryDisplayNameRepository implements DisplayNameRepository {

    public record SaveCall(long id, String value) {}

    private final Map<Long, String> titles = new HashMap<>();
    private final Map<Long, String> nicknames = new HashMap<>();

    private final List<SaveCall> saveDropTitleCalls = new ArrayList<>();
    private final List<SaveCall> saveNicknameCalls = new ArrayList<>();
    private final List<List<Long>> findDropTitlesCalls = new ArrayList<>();
    private final List<List<Long>> findNicknamesCalls = new ArrayList<>();

    private boolean failSave;
    private boolean failFindDropTitles;
    private boolean failFindNicknames;

    @Override
    public void saveDropTitle(long dropId, String title) {
        saveDropTitleCalls.add(new SaveCall(dropId, title));
        if (failSave) {
            throw unavailable("fake saveDropTitle 실패");
        }
        titles.put(dropId, title);
    }

    @Override
    public void saveNickname(long userId, String nickname) {
        saveNicknameCalls.add(new SaveCall(userId, nickname));
        if (failSave) {
            throw unavailable("fake saveNickname 실패");
        }
        nicknames.put(userId, nickname);
    }

    @Override
    public Map<Long, String> findDropTitles(Collection<Long> dropIds) {
        findDropTitlesCalls.add(List.copyOf(dropIds));
        if (failFindDropTitles) {
            throw unavailable("fake findDropTitles 실패");
        }
        return pick(titles, dropIds);
    }

    @Override
    public Map<Long, String> findNicknames(Collection<Long> userIds) {
        findNicknamesCalls.add(List.copyOf(userIds));
        if (failFindNicknames) {
            throw unavailable("fake findNicknames 실패");
        }
        return pick(nicknames, userIds);
    }

    private static Map<Long, String> pick(Map<Long, String> source, Collection<Long> ids) {
        Map<Long, String> result = new HashMap<>();
        for (Long id : ids) {
            if (source.containsKey(id)) {
                result.put(id, source.get(id));
            }
        }
        return result;
    }

    private static RankingStoreUnavailableException unavailable(String message) {
        return new RankingStoreUnavailableException(message, new RuntimeException("simulated redis failure"));
    }

    // ---------------------------------------------------------------- 테스트 조작

    public void givenTitle(long dropId, String title) {
        titles.put(dropId, title);
    }

    public void givenNickname(long userId, String nickname) {
        nicknames.put(userId, nickname);
    }

    public void failSave() {
        this.failSave = true;
    }

    public void failFindDropTitles() {
        this.failFindDropTitles = true;
    }

    public void failFindNicknames() {
        this.failFindNicknames = true;
    }

    // ---------------------------------------------------------------- 상태 조회

    public Map<Long, String> titles() {
        return Map.copyOf(titles);
    }

    public Map<Long, String> nicknames() {
        return Map.copyOf(nicknames);
    }

    public List<SaveCall> saveDropTitleCalls() {
        return List.copyOf(saveDropTitleCalls);
    }

    public List<SaveCall> saveNicknameCalls() {
        return List.copyOf(saveNicknameCalls);
    }

    public List<List<Long>> findDropTitlesCalls() {
        return List.copyOf(findDropTitlesCalls);
    }

    public List<List<Long>> findNicknamesCalls() {
        return List.copyOf(findNicknamesCalls);
    }
}
