package com.queuedrop.queue.application.port;

import java.util.OptionalLong;

/** entry-service 소유 잔여 정원 키 읽기 (D3). 키가 없으면 empty = "아직 초기화 안 됨" */
public interface RemainingCapacityReader {
    OptionalLong read(Long dropId);
}
