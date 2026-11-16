package com.queuedrop.queue.application.service.fake;

import com.queuedrop.queue.domain.model.EnqueueResult;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Redis 대기열 저장소의 인메모리 대역.
 * <ul>
 *   <li>enqueue: 토큰 보유 → alreadyAdmitted, 대기 중 → 현재 rank, 신규 → 맨 뒤에 추가 (enqueue.lua 와 같은 의미)</li>
 *   <li>admit: 앞에서 최대 n 명을 꺼내 토큰 발급. 잔여 정원 확인(admit.lua 안)은 흉내 내지 않는다</li>
 *   <li>active 인덱스: saveMeta 로 등록, removeExpiredDrops(now) 가 closeAt &lt;= now 인 드로우를 뺀다</li>
 *   <li>토큰 TTL 만료는 흉내 내지 않는다 (infra 테스트 범위)</li>
 * </ul>
 */
public class FakeWaitingQueueRepository implements WaitingQueueRepository {

    /** admit 호출 기록 */
    public record AdmitCall(Long dropId, int n, Duration tokenTtl, Instant now) {}

    private final Map<Long, QueueMeta> metas = new HashMap<>();
    private final Set<Long> activeDropIds = new LinkedHashSet<>();
    private final Map<Long, List<Long>> waiting = new HashMap<>();
    private final Map<Long, Set<Long>> tokens = new HashMap<>();
    private final List<AdmitCall> admitCalls = new ArrayList<>();
    private final Set<Long> failingFindMeta = new HashSet<>();
    private final Set<Long> failingAdmit = new HashSet<>();
    private final List<String> calls = new ArrayList<>();
    private Long raceDropId;
    private Long raceUserId;
    private int saveMetaCount;
    private int invocationCount;

    @Override
    public void saveMeta(QueueMeta meta) {
        invocationCount++;
        saveMetaCount++;
        metas.put(meta.dropId(), meta);
        activeDropIds.add(meta.dropId());
    }

    @Override
    public Optional<QueueMeta> findMeta(Long dropId) {
        invocationCount++;
        if (failingFindMeta.contains(dropId)) {
            throw new IllegalStateException("findMeta 실패 (테스트 주입): dropId=" + dropId);
        }
        return Optional.ofNullable(metas.get(dropId));
    }

    @Override
    public EnqueueResult enqueue(Long dropId, Long userId) {
        invocationCount++;
        if (tokensOf(dropId).contains(userId)) {
            return EnqueueResult.alreadyAdmitted();
        }
        List<Long> line = waitingOf(dropId);
        int index = line.indexOf(userId);
        if (index >= 0) {
            return EnqueueResult.waiting(index);
        }
        line.add(userId);
        return EnqueueResult.waiting(line.size() - 1);
    }

    @Override
    public boolean hasToken(Long dropId, Long userId) {
        invocationCount++;
        calls.add("hasToken");
        boolean result = tokensOf(dropId).contains(userId);
        admitIfRacing(dropId, userId);
        return result;
    }

    @Override
    public OptionalLong rank(Long dropId, Long userId) {
        invocationCount++;
        calls.add("rank");
        int index = waitingOf(dropId).indexOf(userId);
        admitIfRacing(dropId, userId);
        return index < 0 ? OptionalLong.empty() : OptionalLong.of(index);
    }

    @Override
    public List<Long> admit(Long dropId, int n, Duration tokenTtl, Instant now) {
        invocationCount++;
        if (failingAdmit.contains(dropId)) {
            throw new IllegalStateException("admit 실패 (테스트 주입): dropId=" + dropId);
        }
        admitCalls.add(new AdmitCall(dropId, n, tokenTtl, now));
        List<Long> line = waitingOf(dropId);
        List<Long> admitted = new ArrayList<>();
        while (admitted.size() < n && !line.isEmpty()) {
            Long userId = line.removeFirst();
            tokensOf(dropId).add(userId);
            admitted.add(userId);
        }
        return admitted;
    }

    @Override
    public List<Long> findActiveDropIds(Instant now) {
        invocationCount++;
        return activeDropIds.stream()
                .filter(id -> metas.containsKey(id) && metas.get(id).closeAt().isAfter(now))
                .toList();
    }

    @Override
    public void removeExpiredDrops(Instant now) {
        invocationCount++;
        activeDropIds.removeIf(id -> metas.containsKey(id) && !metas.get(id).closeAt().isAfter(now));
    }

    // ---- 테스트 준비용 (호출 횟수에 포함하지 않음) ----

    /** 서비스를 거치지 않고 meta 를 둔다 (active 인덱스에도 등록) */
    public void givenMeta(QueueMeta meta) {
        metas.put(meta.dropId(), meta);
        activeDropIds.add(meta.dropId());
    }

    /** 서비스를 거치지 않고 대기열 맨 뒤에 사용자를 세운다 */
    public void givenWaiting(Long dropId, Long... userIds) {
        for (Long userId : userIds) {
            waitingOf(dropId).add(userId);
        }
    }

    /** 서비스를 거치지 않고 입장 토큰을 준다 */
    public void givenToken(Long dropId, Long userId) {
        tokensOf(dropId).add(userId);
    }

    /** 해당 드로우의 findMeta 가 RuntimeException 을 던지게 한다 (Redis 장애 등 흉내) */
    public void failFindMetaFor(Long dropId) {
        failingFindMeta.add(dropId);
    }

    /** 해당 드로우의 admit 이 RuntimeException 을 던지게 한다 */
    public void failAdmitFor(Long dropId) {
        failingAdmit.add(dropId);
    }

    /**
     * 입장 스케줄러와의 경합 흉내: 이 사용자에 대해 hasToken 또는 rank 중 처음 불린 조회가 값을 읽은 "직후"
     * 사용자를 대기열에서 빼고 토큰을 준다 (admit.lua 의 ZPOPMIN + SET token 이 두 조회 사이에 끼어든 상황).
     */
    public void admitRightAfterFirstRead(Long dropId, Long userId) {
        this.raceDropId = dropId;
        this.raceUserId = userId;
    }

    private void admitIfRacing(Long dropId, Long userId) {
        if (dropId.equals(raceDropId) && userId.equals(raceUserId)) {
            raceDropId = null;
            raceUserId = null;
            waitingOf(dropId).remove(userId);
            tokensOf(dropId).add(userId);
        }
    }

    // ---- 결과 상태 조회 (호출 횟수에 포함하지 않음) ----

    public List<Long> waitingUsers(Long dropId) {
        return List.copyOf(waitingOf(dropId));
    }

    public Set<Long> tokenHolders(Long dropId) {
        return Set.copyOf(tokensOf(dropId));
    }

    public Optional<QueueMeta> storedMeta(Long dropId) {
        return Optional.ofNullable(metas.get(dropId));
    }

    public boolean isActive(Long dropId) {
        return activeDropIds.contains(dropId);
    }

    public List<AdmitCall> admitCalls() {
        return List.copyOf(admitCalls);
    }

    /** hasToken / rank 호출 순서 */
    public List<String> readCalls() {
        return List.copyOf(calls);
    }

    public int saveMetaCount() {
        return saveMetaCount;
    }

    /** 포트 메서드 호출 총 횟수 */
    public int invocationCount() {
        return invocationCount;
    }

    private List<Long> waitingOf(Long dropId) {
        return waiting.computeIfAbsent(dropId, id -> new ArrayList<>());
    }

    private Set<Long> tokensOf(Long dropId) {
        return tokens.computeIfAbsent(dropId, id -> new HashSet<>());
    }
}
