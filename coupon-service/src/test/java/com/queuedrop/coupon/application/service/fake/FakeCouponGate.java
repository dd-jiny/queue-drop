package com.queuedrop.coupon.application.service.fake;

import com.queuedrop.coupon.application.port.CompensationResult;
import com.queuedrop.coupon.application.port.CouponGate;
import com.queuedrop.coupon.application.port.GateResult;
import com.queuedrop.coupon.application.port.GateUnavailableException;
import com.queuedrop.coupon.domain.model.IssueOutcome;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Redis 쿠폰 게이트(coupon-*.lua)의 인메모리 대역 (단계 문서 3.3).
 * <ul>
 *   <li>tryIssue (coupon-issue.lua): meta 없음 → NOT_FOUND, now &lt; issueStartAt → NOT_STARTED, 이미 발급 → DUPLICATE,
 *       잔여 0 → SOLD_OUT, 그 외 → remaining-1, issued 추가, requests[userId]=requestId</li>
 *   <li>nextOutcome: 다음 tryIssue 결과를 강제한다. 강제 OK 는 issued·requests 에도 넣는다 (보상·claim 대상이 되도록)</li>
 *   <li>compensate (coupon-compensate.lua): persisted 면 ALREADY_PERSISTED, requests[userId] ≠ requestId 면 NOT_OURS,
 *       그 외 issued·requests 제거 + remaining+1 → COMPENSATED</li>
 *   <li>claimForSave (coupon-claim.lua): issued 이고 requests[userId] == requestId 일 때만 persisted 에 넣고 true (멱등)</li>
 *   <li>initialize: SET NX (이미 있으면 무변화). remaining(port): 잔여 수량 (meta 없으면 0)</li>
 *   <li>모든 호출은 예외를 던지는 경우에도 기록한다</li>
 * </ul>
 */
public class FakeCouponGate implements CouponGate {

    /** tryIssue·compensate·claimForSave 호출 기록. requestId 까지 남긴다 */
    public record Call(long couponEventId, long userId, String requestId) {}

    public record InitializeCall(long couponEventId, int totalQuantity, Instant issueStartAt) {}

    private record Meta(int totalQuantity, Instant issueStartAt) {}

    private final Map<Long, Meta> metas = new HashMap<>();
    private final Map<Long, Integer> remaining = new HashMap<>();
    private final Map<Long, Set<Long>> issued = new HashMap<>();
    private final Map<Long, Map<Long, String>> requests = new HashMap<>();
    private final Map<Long, Set<Long>> persisted = new HashMap<>();

    private final Deque<GateResult> forcedOutcomes = new ArrayDeque<>();
    private final Deque<CompensationResult> forcedCompensations = new ArrayDeque<>();
    private final Deque<Boolean> forcedClaims = new ArrayDeque<>();

    private final List<Call> tryIssueCalls = new ArrayList<>();
    private final List<Call> compensateCalls = new ArrayList<>();
    private final List<CompensationResult> compensateResults = new ArrayList<>();
    private final List<Call> claimCalls = new ArrayList<>();
    private final List<InitializeCall> initializeCalls = new ArrayList<>();
    private final List<Long> remainingCalls = new ArrayList<>();

    private boolean timeoutAfterIssueNext;
    private boolean timeoutBeforeIssueNext;
    private boolean failCompensateNext;
    private boolean failInitializeNext;
    private boolean failRemaining;
    private GateUnavailableException lastTryIssueFailure;
    private GateUnavailableException lastCompensateFailure;
    private GateUnavailableException lastInitializeFailure;

    // ---------------------------------------------------------------- 포트 구현

    @Override
    public void initialize(long couponEventId, int totalQuantity, Instant issueStartAt) {
        initializeCalls.add(new InitializeCall(couponEventId, totalQuantity, issueStartAt));
        if (failInitializeNext) {
            failInitializeNext = false;
            lastInitializeFailure = unavailable("fake initialize 실패");
            throw lastInitializeFailure;
        }
        if (metas.containsKey(couponEventId)) {
            return;     // SET NX: 진행 중인 잔여 수량을 덮어쓰지 않는다
        }
        metas.put(couponEventId, new Meta(totalQuantity, issueStartAt));
        remaining.put(couponEventId, totalQuantity);
    }

    @Override
    public GateResult tryIssue(long couponEventId, long userId, String requestId, Instant now) {
        tryIssueCalls.add(new Call(couponEventId, userId, requestId));
        if (timeoutBeforeIssueNext) {
            // Lua 가 실행되지 않은 채 실패 (연결 실패 등)
            timeoutBeforeIssueNext = false;
            lastTryIssueFailure = unavailable("fake tryIssue 실패 (Lua 미실행)");
            throw lastTryIssueFailure;
        }
        GateResult result = decide(couponEventId, userId, requestId, now);
        if (timeoutAfterIssueNext) {
            // Lua 는 실행돼 상태가 바뀌었는데 응답만 타임아웃 (E-41)
            timeoutAfterIssueNext = false;
            lastTryIssueFailure = unavailable("fake tryIssue 응답 타임아웃 (Lua 실행됨)");
            throw lastTryIssueFailure;
        }
        return result;
    }

    private GateResult decide(long couponEventId, long userId, String requestId, Instant now) {
        if (!forcedOutcomes.isEmpty()) {
            GateResult forced = forcedOutcomes.poll();
            if (forced.outcome() == IssueOutcome.OK) {
                markIssued(couponEventId, userId, requestId);
            }
            return forced;
        }
        Meta meta = metas.get(couponEventId);
        if (meta == null) {
            return new GateResult(IssueOutcome.NOT_FOUND);
        }
        if (now.isBefore(meta.issueStartAt())) {
            return new GateResult(IssueOutcome.NOT_STARTED);
        }
        if (issuedOf(couponEventId).contains(userId)) {
            return new GateResult(IssueOutcome.DUPLICATE);
        }
        if (remaining.getOrDefault(couponEventId, 0) <= 0) {
            return new GateResult(IssueOutcome.SOLD_OUT);
        }
        remaining.merge(couponEventId, -1, Integer::sum);
        markIssued(couponEventId, userId, requestId);
        return new GateResult(IssueOutcome.OK);
    }

    @Override
    public CompensationResult compensate(long couponEventId, long userId, String requestId) {
        compensateCalls.add(new Call(couponEventId, userId, requestId));
        if (failCompensateNext) {
            failCompensateNext = false;
            lastCompensateFailure = unavailable("fake compensate 실패");
            throw lastCompensateFailure;
        }
        CompensationResult result = forcedCompensations.isEmpty()
                ? compensateState(couponEventId, userId, requestId)
                : forcedCompensations.poll();
        compensateResults.add(result);
        return result;
    }

    private CompensationResult compensateState(long couponEventId, long userId, String requestId) {
        if (persisted.getOrDefault(couponEventId, Set.of()).contains(userId)) {
            return CompensationResult.ALREADY_PERSISTED;
        }
        if (!Objects.equals(requests.getOrDefault(couponEventId, Map.of()).get(userId), requestId)) {
            return CompensationResult.NOT_OURS;
        }
        issuedOf(couponEventId).remove(userId);
        requests.get(couponEventId).remove(userId);
        remaining.merge(couponEventId, 1, Integer::sum);
        return CompensationResult.COMPENSATED;
    }

    @Override
    public boolean claimForSave(long couponEventId, long userId, String requestId) {
        claimCalls.add(new Call(couponEventId, userId, requestId));
        if (!forcedClaims.isEmpty()) {
            return forcedClaims.poll();
        }
        if (!issuedOf(couponEventId).contains(userId)) {
            return false;
        }
        if (!Objects.equals(requests.getOrDefault(couponEventId, Map.of()).get(userId), requestId)) {
            return false;
        }
        persisted.computeIfAbsent(couponEventId, k -> new LinkedHashSet<>()).add(userId);
        return true;
    }

    @Override
    public long remaining(long couponEventId) {
        remainingCalls.add(couponEventId);
        if (failRemaining) {
            throw unavailable("fake remaining 실패");
        }
        return remaining.getOrDefault(couponEventId, 0);
    }

    private void markIssued(long couponEventId, long userId, String requestId) {
        issuedOf(couponEventId).add(userId);
        requests.computeIfAbsent(couponEventId, k -> new HashMap<>()).put(userId, requestId);
    }

    private Set<Long> issuedOf(long couponEventId) {
        return issued.computeIfAbsent(couponEventId, k -> new LinkedHashSet<>());
    }

    private static GateUnavailableException unavailable(String message) {
        return new GateUnavailableException(message, new RuntimeException("simulated redis failure"));
    }

    // ---------------------------------------------------------------- 테스트 조작

    /** 다음 tryIssue 결과를 강제한다. 여러 번 호출하면 순서대로 소비된다. */
    public void nextOutcome(IssueOutcome outcome) {
        forcedOutcomes.add(new GateResult(outcome));
    }

    /** 다음 compensate 결과를 강제한다 (상태는 바꾸지 않는다). */
    public void nextCompensation(CompensationResult result) {
        forcedCompensations.add(result);
    }

    /** 다음 claimForSave 결과를 강제한다 (상태는 바꾸지 않는다). */
    public void nextClaim(boolean result) {
        forcedClaims.add(result);
    }

    /** 다음 tryIssue 한 번: 판정을 실행해 상태를 바꾼 뒤 GateUnavailableException (응답 타임아웃) */
    public void timeoutAfterIssueNext() {
        this.timeoutAfterIssueNext = true;
    }

    /** 다음 tryIssue 한 번: 상태 변화 없이 GateUnavailableException (Lua 미실행) */
    public void timeoutBeforeIssueNext() {
        this.timeoutBeforeIssueNext = true;
    }

    /** 다음 compensate 한 번이 GateUnavailableException 을 던진다 (호출 기록은 남는다) */
    public void failCompensateNext() {
        this.failCompensateNext = true;
    }

    /** 다음 initialize 한 번이 GateUnavailableException 을 던진다 (호출 기록은 남고 상태는 그대로) */
    public void failInitializeNext() {
        this.failInitializeNext = true;
    }

    /** 이후 모든 remaining(port) 호출이 GateUnavailableException 을 던진다 */
    public void failRemaining() {
        this.failRemaining = true;
    }

    /** 판정을 거치지 않고 requestId 요청으로 발급된 상태를 만든다 (issued + requests). 저장 유스케이스 준비용 */
    public void givenIssued(long couponEventId, long userId, String requestId) {
        markIssued(couponEventId, userId, requestId);
    }

    // ---------------------------------------------------------------- 상태 조회 (실패 주입 영향 없음)

    public List<Call> tryIssueCalls() {
        return List.copyOf(tryIssueCalls);
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

    public List<Long> remainingCalls() {
        return List.copyOf(remainingCalls);
    }

    public GateUnavailableException lastTryIssueFailure() {
        return lastTryIssueFailure;
    }

    public GateUnavailableException lastCompensateFailure() {
        return lastCompensateFailure;
    }

    public GateUnavailableException lastInitializeFailure() {
        return lastInitializeFailure;
    }

    public boolean isInitialized(long couponEventId) {
        return metas.containsKey(couponEventId);
    }

    /** 잔여 수량 상태 (port remaining 과 달리 실패 주입·호출 기록 없음) */
    public int remainingOf(long couponEventId) {
        return remaining.getOrDefault(couponEventId, 0);
    }

    public Set<Long> issued(long couponEventId) {
        return Set.copyOf(issued.getOrDefault(couponEventId, Set.of()));
    }

    /** userId → requestId */
    public Map<Long, String> requests(long couponEventId) {
        return Map.copyOf(requests.getOrDefault(couponEventId, Map.of()));
    }

    public Set<Long> persisted(long couponEventId) {
        return Set.copyOf(persisted.getOrDefault(couponEventId, Set.of()));
    }
}
