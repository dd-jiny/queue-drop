package com.queuedrop.entry.domain.service;

import com.queuedrop.entry.domain.exception.DomainException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** 추첨 순수 함수. 같은 입력 순서 + 같은 시드 = 같은 결과. */
public final class Drawer {

    /**
     * @param orderedUserIds entry_seq 오름차순으로 정렬된 응모자 목록 (결정적 입력)
     * @param winnerCount    K (≥ 1)
     * @param seed           셔플 시드 (draw_results 에 저장되어 재현에 사용)
     * @return 당첨자 userId 집합, 크기 = min(K, 응모자 수)
     */
    public Set<Long> draw(List<Long> orderedUserIds, int winnerCount, long seed) {
        if (winnerCount < 1) {
            throw new DomainException("winnerCount 는 1 이상이어야 합니다: winnerCount=" + winnerCount);
        }
        List<Long> shuffled = new ArrayList<>(orderedUserIds);   // 입력 불변 보장
        Collections.shuffle(shuffled, new Random(seed));          // Fisher–Yates
        int k = Math.min(winnerCount, shuffled.size());
        return Set.copyOf(shuffled.subList(0, k));
    }
}
