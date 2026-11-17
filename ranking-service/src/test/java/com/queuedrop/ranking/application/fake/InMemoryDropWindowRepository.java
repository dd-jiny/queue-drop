package com.queuedrop.ranking.application.fake;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import com.queuedrop.ranking.domain.model.DropWindow;
import com.queuedrop.ranking.domain.repository.DropWindowRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** rank:drop:closeAt Hash 의 인메모리 대역. findAll 은 없는 id 를 closeAt=null 로 채운다. fail*() 이후 해당 호출은 모두 실패 */
public class InMemoryDropWindowRepository implements DropWindowRepository {

    public record SaveCall(long dropId, Instant closeAt) {}

    private final Map<Long, Instant> closeAts = new HashMap<>();

    private final List<SaveCall> saveCalls = new ArrayList<>();
    private final List<List<Long>> findAllCalls = new ArrayList<>();

    private final java.util.Set<Long> omitted = new java.util.HashSet<>();

    private boolean failSave;
    private boolean failFindAll;

    @Override
    public void save(long dropId, Instant closeAt) {
        saveCalls.add(new SaveCall(dropId, closeAt));
        if (failSave) {
            throw unavailable("fake save 실패");
        }
        closeAts.put(dropId, closeAt);
    }

    @Override
    public Map<Long, DropWindow> findAll(Collection<Long> dropIds) {
        findAllCalls.add(List.copyOf(dropIds));
        if (failFindAll) {
            throw unavailable("fake findAll 실패");
        }
        Map<Long, DropWindow> result = new HashMap<>();
        for (Long id : dropIds) {
            if (omitted.contains(id)) {
                continue;   // 계약 위반 재현: 없는 id 를 채우지 않고 빠뜨린다
            }
            result.put(id, new DropWindow(id, closeAts.get(id)));
        }
        return result;
    }

    private static RankingStoreUnavailableException unavailable(String message) {
        return new RankingStoreUnavailableException(message, new RuntimeException("simulated redis failure"));
    }

    // ---------------------------------------------------------------- 테스트 조작

    public void givenCloseAt(long dropId, Instant closeAt) {
        closeAts.put(dropId, closeAt);
    }

    /** findAll 결과 맵에서 이 id 를 빠뜨린다 (포트 계약 위반 방어 검증용) */
    public void omitFromFindAll(long dropId) {
        omitted.add(dropId);
    }

    public void failSave() {
        this.failSave = true;
    }

    public void failFindAll() {
        this.failFindAll = true;
    }

    // ---------------------------------------------------------------- 상태 조회

    public Map<Long, Instant> closeAts() {
        return Map.copyOf(closeAts);
    }

    public List<SaveCall> saveCalls() {
        return List.copyOf(saveCalls);
    }

    public List<List<Long>> findAllCalls() {
        return List.copyOf(findAllCalls);
    }
}
