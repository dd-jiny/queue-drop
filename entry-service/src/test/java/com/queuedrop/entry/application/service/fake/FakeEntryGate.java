package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.port.CompensationResult;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.application.port.GateUnavailableException;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Redis 응모 게이트(entry-*.lua)의 인메모리 대역 (단계 문서 4.1, v1.5).
 * <ul>
 *   <li>tryApply: meta 없음·기간 밖 → CLOSED, 토큰 없음 → NO_TOKEN, 이미 응모 → DUPLICATE, 잔여 0 → SOLD_OUT,
 *       그 외 → remaining-1, 응모자 추가, requests[userId]=eventId, seq+1 (entry-apply.lua 와 같은 판정 순서)</li>
 *   <li>nextOutcome: 다음 tryApply 결과를 강제한다. 강제 OK 는 응모자 Set·requests 에도 넣는다 (보상·claim 대상이 되도록)</li>
 *   <li>compensate (entry-compensate.lua): persisted 면 ALREADY_PERSISTED, requests[userId] ≠ eventId 면 NOT_OURS,
 *       그 외 응모자·requests 제거 + remaining+1 → COMPENSATED. seq 는 되돌리지 않는다</li>
 *   <li>claimForSave (entry-claim.lua): 응모자이고 requests[userId] == eventId 일 때만 persisted 에 넣고 true (멱등)</li>
 *   <li>tryApply·compensate·claimForSave 호출은 eventId 까지 항상 기록한다 (예외를 던지는 경우 포함)</li>
 *   <li>close: 이후 그 드로우의 tryApply 는 CLOSED (meta.state=CLOSED). expireAfterDraw: 호출만 기록 (상태는 남긴다)</li>
 *   <li>close·expireAfterDraw 는 호출을 항상 기록하고, 실패 주입 시 한 번 GateUnavailableException 을 던진다</li>
 * </ul>
 */
public class FakeEntryGate implements EntryGate {

    /** 호출 기록. eventId 까지 남긴다 */
    public record Call(long dropId, long userId, String eventId) {}

    public record InitializeCall(long dropId, int capacity, Instant openAt, Instant closeAt) {}

    public record ExpireCall(long dropId, Duration ttl) {}

    private record Key(long dropId, long userId) {}

    private record Meta(int capacity, Instant openAt, Instant closeAt) {}

    private final Map<Long, Meta> metas = new HashMap<>();
    private final Map<Long, Integer> remaining = new HashMap<>();
    private final Map<Long, Integer> seq = new HashMap<>();
    private final Map<Long, Set<Long>> applicants = new HashMap<>();
    private final Map<Long, Map<Long, String>> requests = new HashMap<>();
    private final Map<Long, Set<Long>> persisted = new HashMap<>();
    private final Set<Key> tokens = new HashSet<>();
    private final Deque<GateResult> forced = new ArrayDeque<>();
    private final Deque<CompensationResult> forcedCompensations = new ArrayDeque<>();
    private final List<Call> tryApplyCalls = new ArrayList<>();
    private final List<Call> compensateCalls = new ArrayList<>();
    private final List<CompensationResult> compensateResults = new ArrayList<>();
    private final List<Call> claimCalls = new ArrayList<>();
    private final List<InitializeCall> initializeCalls = new ArrayList<>();
    private boolean timeoutAfterApplyNext;
    private boolean timeoutBeforeApplyNext;
    private boolean failCompensateNext;
    private GateUnavailableException lastTryApplyFailure;
    private RuntimeException initializeFailure;
    private final Set<Long> closed = new HashSet<>();
    private final List<Long> closeCalls = new ArrayList<>();
    private final List<ExpireCall> expireCalls = new ArrayList<>();
    private boolean failCloseNext;
    private boolean failExpireNext;

    @Override
    public GateResult tryApply(long dropId, long userId, String eventId, Instant now) {
        tryApplyCalls.add(new Call(dropId, userId, eventId));
        if (timeoutBeforeApplyNext) {
            // Lua 가 실행되지 않은 채 응답 실패 (연결 실패 등)
            timeoutBeforeApplyNext = false;
            throw tryApplyFailure("fake tryApply 실패 (Lua 미실행)");
        }
        GateResult result = decide(dropId, userId, eventId, now);
        if (timeoutAfterApplyNext) {
            // Lua 는 실행돼 상태가 바뀌었는데 응답만 타임아웃 (E-41)
            timeoutAfterApplyNext = false;
            throw tryApplyFailure("fake tryApply 응답 타임아웃 (Lua 실행됨)");
        }
        return result;
    }

    private GateResult decide(long dropId, long userId, String eventId, Instant now) {
        if (!forced.isEmpty()) {
            GateResult result = forced.poll();
            if (result.isOk()) {
                markApplied(dropId, userId, eventId);
            }
            return result;
        }
        Meta meta = metas.get(dropId);
        if (closed.contains(dropId)) {
            return GateResult.rejected(ApplyOutcome.CLOSED);
        }
        if (meta == null || now.isBefore(meta.openAt()) || !now.isBefore(meta.closeAt())) {
            return GateResult.rejected(ApplyOutcome.CLOSED);
        }
        if (!tokens.contains(new Key(dropId, userId))) {
            return GateResult.rejected(ApplyOutcome.NO_TOKEN);
        }
        if (applicantsOf(dropId).contains(userId)) {
            return GateResult.rejected(ApplyOutcome.DUPLICATE);
        }
        if (remaining.getOrDefault(dropId, 0) <= 0) {
            return GateResult.rejected(ApplyOutcome.SOLD_OUT);
        }
        remaining.merge(dropId, -1, Integer::sum);
        markApplied(dropId, userId, eventId);
        return GateResult.ok(seq.merge(dropId, 1, Integer::sum));
    }

    private void markApplied(long dropId, long userId, String eventId) {
        applicantsOf(dropId).add(userId);
        requests.computeIfAbsent(dropId, k -> new HashMap<>()).put(userId, eventId);
    }

    private GateUnavailableException tryApplyFailure(String message) {
        lastTryApplyFailure = new GateUnavailableException(message, new RuntimeException("simulated redis timeout"));
        return lastTryApplyFailure;
    }

    @Override
    public CompensationResult compensate(long dropId, long userId, String eventId) {
        compensateCalls.add(new Call(dropId, userId, eventId));
        if (failCompensateNext) {
            failCompensateNext = false;
            throw new GateUnavailableException("fake compensate 실패", new RuntimeException("simulated redis down"));
        }
        CompensationResult result = forcedCompensations.isEmpty()
                ? compensateState(dropId, userId, eventId)
                : forcedCompensations.poll();
        compensateResults.add(result);
        return result;
    }

    private CompensationResult compensateState(long dropId, long userId, String eventId) {
        if (persisted.getOrDefault(dropId, Set.of()).contains(userId)) {
            return CompensationResult.ALREADY_PERSISTED;
        }
        Map<Long, String> dropRequests = requests.getOrDefault(dropId, Map.of());
        if (!Objects.equals(dropRequests.get(userId), eventId)) {
            return CompensationResult.NOT_OURS;
        }
        applicantsOf(dropId).remove(userId);
        requests.get(dropId).remove(userId);
        remaining.merge(dropId, 1, Integer::sum);
        return CompensationResult.COMPENSATED;
    }

    @Override
    public boolean claimForSave(long dropId, long userId, String eventId) {
        claimCalls.add(new Call(dropId, userId, eventId));
        if (!applicantsOf(dropId).contains(userId)) {
            return false;
        }
        if (!Objects.equals(requests.getOrDefault(dropId, Map.of()).get(userId), eventId)) {
            return false;
        }
        persisted.computeIfAbsent(dropId, k -> new LinkedHashSet<>()).add(userId);
        return true;
    }

    @Override
    public boolean isApplicant(long dropId, long userId) {
        return applicants.getOrDefault(dropId, Set.of()).contains(userId);
    }

    @Override
    public void initialize(long dropId, int capacity, Instant openAt, Instant closeAt) {
        initializeCalls.add(new InitializeCall(dropId, capacity, openAt, closeAt));
        if (initializeFailure != null) {
            RuntimeException failure = initializeFailure;
            initializeFailure = null; // 한 번만 실패 (재전달 복구 검증용)
            throw failure;
        }
        if (metas.containsKey(dropId)) {
            return;
        }
        metas.put(dropId, new Meta(capacity, openAt, closeAt));
        remaining.put(dropId, capacity);
        seq.putIfAbsent(dropId, 0);
    }

    @Override
    public void close(long dropId) {
        closeCalls.add(dropId);
        if (failCloseNext) {
            failCloseNext = false;
            throw new GateUnavailableException("fake close 실패", new RuntimeException("simulated redis down"));
        }
        closed.add(dropId);
    }

    @Override
    public void expireAfterDraw(long dropId, Duration ttl) {
        expireCalls.add(new ExpireCall(dropId, ttl));
        if (failExpireNext) {
            failExpireNext = false;
            throw new GateUnavailableException("fake expireAfterDraw 실패", new RuntimeException("simulated redis down"));
        }
    }

    private Set<Long> applicantsOf(long dropId) {
        return applicants.computeIfAbsent(dropId, k -> new LinkedHashSet<>());
    }

    // ---- 테스트 조작 ----

    /** 다음 tryApply 결과를 강제한다. 여러 번 호출하면 순서대로 소비된다. */
    public void nextOutcome(GateResult result) {
        forced.add(result);
    }

    /** 다음 compensate 결과를 강제한다 (상태는 바꾸지 않는다). 여러 번 호출하면 순서대로 소비된다. */
    public void nextCompensation(CompensationResult result) {
        forcedCompensations.add(result);
    }

    /** 다음 tryApply 한 번: 판정은 실행해 상태를 바꾼 뒤 GateUnavailableException 을 던진다 (응답 타임아웃) */
    public void timeoutAfterApplyNext() {
        this.timeoutAfterApplyNext = true;
    }

    /** 다음 tryApply 한 번: 상태 변화 없이 GateUnavailableException 을 던진다 (Lua 미실행) */
    public void timeoutBeforeApplyNext() {
        this.timeoutBeforeApplyNext = true;
    }

    /** 다음 compensate 한 번이 GateUnavailableException 을 던진다 (호출 기록은 남는다). */
    public void failCompensateNext() {
        this.failCompensateNext = true;
    }

    public void grantToken(long dropId, long userId) {
        tokens.add(new Key(dropId, userId));
    }

    /** 판정을 거치지 않고 응모자로만 등록한다 (requests 기록 없음 — claimForSave 는 false). 조회 유스케이스 준비용. */
    public void givenApplicant(long dropId, long userId) {
        applicantsOf(dropId).add(userId);
    }

    /** 판정을 거치지 않고 eventId 요청으로 응모된 상태를 만든다 (applicants + requests). 저장 유스케이스 준비용. */
    public void givenApplicant(long dropId, long userId, String eventId) {
        markApplied(dropId, userId, eventId);
    }

    /** 다음 initialize 호출 한 번이 이 예외를 던진다 (호출 기록은 남는다). */
    public void failNextInitializeWith(RuntimeException failure) {
        this.initializeFailure = failure;
    }

    /** 다음 close 한 번이 GateUnavailableException 을 던진다 (호출 기록은 남고, 상태는 바뀌지 않는다). */
    public void failCloseNext() {
        this.failCloseNext = true;
    }

    /** 다음 expireAfterDraw 한 번이 GateUnavailableException 을 던진다 (호출 기록은 남는다). */
    public void failExpireAfterDrawNext() {
        this.failExpireNext = true;
    }

    // ---- 상태 조회 ----

    /** close 호출된 dropId (실패한 호출 포함, 호출 순서) */
    public List<Long> closeCalls() {
        return List.copyOf(closeCalls);
    }

    /** close 가 성공해 CLOSED 상태인지 */
    public boolean isClosed(long dropId) {
        return closed.contains(dropId);
    }

    /** expireAfterDraw 호출 기록 (실패한 호출 포함) */
    public List<ExpireCall> expireCalls() {
        return List.copyOf(expireCalls);
    }

    public List<Call> tryApplyCalls() {
        return List.copyOf(tryApplyCalls);
    }

    public List<Call> compensateCalls() {
        return List.copyOf(compensateCalls);
    }

    /** compensate 가 정상 반환한 결과 (예외를 던진 호출은 빠진다) */
    public List<CompensationResult> compensateResults() {
        return List.copyOf(compensateResults);
    }

    public List<Call> claimCalls() {
        return List.copyOf(claimCalls);
    }

    public List<InitializeCall> initializeCalls() {
        return List.copyOf(initializeCalls);
    }

    /** 마지막으로 tryApply 가 던진 GateUnavailableException (원래 예외가 가려지지 않았는지 확인용) */
    public GateUnavailableException lastTryApplyFailure() {
        return lastTryApplyFailure;
    }

    public int remaining(long dropId) {
        return remaining.getOrDefault(dropId, 0);
    }

    public int seq(long dropId) {
        return seq.getOrDefault(dropId, 0);
    }

    public Set<Long> applicants(long dropId) {
        return Set.copyOf(applicants.getOrDefault(dropId, Set.of()));
    }

    /** userId → eventId */
    public Map<Long, String> requests(long dropId) {
        return Map.copyOf(requests.getOrDefault(dropId, Map.of()));
    }

    public Set<Long> persisted(long dropId) {
        return Set.copyOf(persisted.getOrDefault(dropId, Set.of()));
    }
}
