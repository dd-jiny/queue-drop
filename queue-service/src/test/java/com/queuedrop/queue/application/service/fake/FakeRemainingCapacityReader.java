package com.queuedrop.queue.application.service.fake;

import com.queuedrop.queue.application.port.RemainingCapacityReader;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/** entry 소유 잔여 정원 키를 흉내 낸다. set 하지 않은 드로우는 "키 없음"(empty). */
public class FakeRemainingCapacityReader implements RemainingCapacityReader {

    private final Map<Long, Long> remaining = new HashMap<>();

    @Override
    public OptionalLong read(Long dropId) {
        Long value = remaining.get(dropId);
        return value == null ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public void set(Long dropId, long value) {
        remaining.put(dropId, value);
    }
}
