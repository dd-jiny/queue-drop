package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.application.port.GateResult;
import com.queuedrop.entry.domain.model.ApplyOutcome;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis 응모 게이트(entry-*.lua)의 인메모리 대역.
 * <ul>
 *   <li>tryApply: meta 없음·기간 밖 → CLOSED, 토큰 없음 → NO_TOKEN, 이미 응모 → DUPLICATE, 잔여 0 → SOLD_OUT,
 *       그 외 → remaining-1, 응모자 추가, seq+1 (entry-apply.lua 와 같은 판정 순서)</li>
 *   <li>nextOutcome: 다음 tryApply 결과를 강제한다. 강제 OK 는 응모자 Set 에도 넣는다 (보상 대상이 되도록)</li>
 *   <li>compensate: 응모자에서 빠졌을 때만 remaining+1. seq 는 되돌리지 않는다. 호출은 항상 기록</li>
 * </ul>
 */
public class FakeEntryGate implements EntryGate {

    public record Call(long dropId, long userId) {}

    public record InitializeCall(long dropId, int capacity, Instant openAt, Instant closeAt) {}

    private record Meta(int capacity, Instant openAt, Instant closeAt) {}

    private final Map<Long, Meta> metas = new HashMap<>();
    private final Map<Long, Integer> remaining = new HashMap<>();
    private final Map<Long, Integer> seq = new HashMap<>();
    private final Map<Long, Set<Long>> applicants = new HashMap<>();
    private final Set<Call> tokens = new HashSet<>();
    private final Deque<GateResult> forced = new ArrayDeque<>();
    private final List<Call> compensateCalls = new ArrayList<>();
    private final List<InitializeCall> initializeCalls = new ArrayList<>();
    private RuntimeException compensateFailure;
    private RuntimeException initializeFailure;

    @Override
    public GateResult tryApply(long dropId, long userId, Instant now) {
        if (!forced.isEmpty()) {
            GateResult result = forced.poll();
            if (result.isOk()) {
                applicants.computeIfAbsent(dropId, k -> new LinkedHashSet<>()).add(userId);
            }
            return result;
        }
        Meta meta = metas.get(dropId);
        if (meta == null || now.isBefore(meta.openAt()) || !now.isBefore(meta.closeAt())) {
            return GateResult.rejected(ApplyOutcome.CLOSED);
        }
        if (!tokens.contains(new Call(dropId, userId))) {
            return GateResult.rejected(ApplyOutcome.NO_TOKEN);
        }
        Set<Long> users = applicants.computeIfAbsent(dropId, k -> new LinkedHashSet<>());
        if (users.contains(userId)) {
            return GateResult.rejected(ApplyOutcome.DUPLICATE);
        }
        if (remaining.getOrDefault(dropId, 0) <= 0) {
            return GateResult.rejected(ApplyOutcome.SOLD_OUT);
        }
        remaining.merge(dropId, -1, Integer::sum);
        users.add(userId);
        return GateResult.ok(seq.merge(dropId, 1, Integer::sum));
    }

    @Override
    public void compensate(long dropId, long userId) {
        compensateCalls.add(new Call(dropId, userId));
        if (compensateFailure != null) {
            throw compensateFailure;
        }
        Set<Long> users = applicants.get(dropId);
        if (users != null && users.remove(userId)) {
            remaining.merge(dropId, 1, Integer::sum);
        }
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

    // ---- 테스트 조작 ----

    /** 다음 tryApply 결과를 강제한다. 여러 번 호출하면 순서대로 소비된다. */
    public void nextOutcome(GateResult result) {
        forced.add(result);
    }

    public void grantToken(long dropId, long userId) {
        tokens.add(new Call(dropId, userId));
    }

    /** 판정을 거치지 않고 응모자로 등록한다 (저장·조회 유스케이스 준비용). */
    public void givenApplicant(long dropId, long userId) {
        applicants.computeIfAbsent(dropId, k -> new LinkedHashSet<>()).add(userId);
    }

    /** 이후 compensate 호출이 이 예외를 던진다 (호출 기록은 남는다). */
    public void failCompensateWith(RuntimeException failure) {
        this.compensateFailure = failure;
    }

    /** 다음 initialize 호출 한 번이 이 예외를 던진다 (호출 기록은 남는다). */
    public void failNextInitializeWith(RuntimeException failure) {
        this.initializeFailure = failure;
    }

    // ---- 상태 조회 ----

    public List<Call> compensateCalls() {
        return List.copyOf(compensateCalls);
    }

    public List<InitializeCall> initializeCalls() {
        return List.copyOf(initializeCalls);
    }

    public int remaining(long dropId) {
        return remaining.getOrDefault(dropId, 0);
    }

    public Set<Long> applicants(long dropId) {
        return Set.copyOf(applicants.getOrDefault(dropId, Set.of()));
    }
}
