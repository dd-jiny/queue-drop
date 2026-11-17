package com.queuedrop.entry.application.service.fake;

import com.queuedrop.entry.application.port.SeedGenerator;
import java.util.ArrayDeque;
import java.util.Deque;

/** 고정 시드 대역. then(...) 으로 넣은 값을 순서대로 내고, 다 쓰면 기본 시드를 계속 낸다. */
public class FixedSeedGenerator implements SeedGenerator {

    private final long defaultSeed;
    private final Deque<Long> queued = new ArrayDeque<>();
    private int calls;

    public FixedSeedGenerator(long defaultSeed) {
        this.defaultSeed = defaultSeed;
    }

    @Override
    public long next() {
        calls++;
        return queued.isEmpty() ? defaultSeed : queued.poll();
    }

    public FixedSeedGenerator then(long seed) {
        queued.add(seed);
        return this;
    }

    public int calls() {
        return calls;
    }
}
