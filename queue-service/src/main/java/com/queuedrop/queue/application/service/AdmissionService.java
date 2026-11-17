package com.queuedrop.queue.application.service;

import com.queuedrop.queue.application.config.AdmissionProperties;
import com.queuedrop.queue.application.port.SchedulerGate;
import com.queuedrop.queue.domain.model.QueueMeta;
import com.queuedrop.queue.domain.repository.WaitingQueueRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 주기적 입장 처리. 잔여 정원 확인은 저장소의 admit(원자 Lua) 안에서 한다 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AdmissionService {

    static final String GATE_NAME = "admission";

    private final WaitingQueueRepository queueRepository;
    private final SchedulerGate schedulerGate;
    private final AdmissionProperties props;
    private final Clock clock;

    /** @return 이번 주기에 입장한 총 인원 (게이트 미통과 시 0). 배치 크기는 드로우마다 적용 */
    public int admit() {
        if (!schedulerGate.tryPass(GATE_NAME, Duration.ofMillis(props.gateWindowMs()))) return 0;
        Instant now = clock.instant();
        queueRepository.removeExpiredDrops(now);
        Duration tokenTtl = Duration.ofSeconds(props.tokenTtlSeconds());
        int total = 0;
        for (Long dropId : queueRepository.findActiveDropIds(now)) {
            total += admitDrop(dropId, tokenTtl, now);
        }
        return total;
    }

    /** 한 드로우의 실패(Redis 일시 오류, entry 소유 키 형식 오류 등)가 다른 드로우의 입장을 막지 않도록 격리한다 */
    private int admitDrop(Long dropId, Duration tokenTtl, Instant now) {
        try {
            Optional<QueueMeta> meta = queueRepository.findMeta(dropId);
            if (meta.isEmpty() || !meta.get().isOpen(now)) return 0;
            List<Long> admitted = queueRepository.admit(dropId, props.batchSize(), tokenTtl, now);
            if (!admitted.isEmpty()) log.debug("dropId={} admitted={}", dropId, admitted.size());
            return admitted.size();
        } catch (RuntimeException e) {
            // 매 주기 반복될 수 있어 스택 없이 남긴다
            log.error("admission failed: dropId={} - {}", dropId, e.toString());
            return 0;
        }
    }
}
