package com.queuedrop.queue.presentation.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;

import com.queuedrop.common.event.DropCreated;
import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.application.dto.QueueMetaRegisterCommand;
import com.queuedrop.queue.application.service.QueueMetaRegisterService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 리스너 재시도 정책 (architecture.md·D4): queue 는 저장 실패 시 1초 간격 3회 재시도 후 로그·스킵 (DLT 없음).
 * 저장(Redis) 실패를 흉내 내려고 QueueMetaRegisterService 를 Mock 으로 바꾼다.
 */
@SpringBootTest(properties = "queue.admission.scheduler-enabled=false")
@Import({RedisContainerConfig.class, KafkaContainerConfig.class,
        DropCreatedListenerIntegrationTest.TopicConfig.class})
class DropCreatedListenerRetryIntegrationTest {

    private static final Long FAILING_DROP = 201L;
    private static final Long NEXT_DROP = 202L;

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @MockitoBean QueueMetaRegisterService registerService;

    private final Map<Long, List<Instant>> calls = new ConcurrentHashMap<>();

    private void publish(Long dropId) throws Exception {
        Instant openAt = Instant.now().plus(Duration.ofHours(1));
        DropCreated event = new DropCreated(dropId, 3L, "한정판 스니커즈", 100, 10, openAt, openAt.plus(Duration.ofHours(2)));
        kafkaTemplate.send(Topics.DROP_CREATED, String.valueOf(dropId), event).get(5, TimeUnit.SECONDS);
    }

    private List<Instant> callsOf(Long dropId) {
        return calls.getOrDefault(dropId, List.of());
    }

    @Test
    @DisplayName("[T3-22] 저장이 계속 실패하면 1초 간격으로 3회 재시도(총 4회) 후 건너뛰고 다음 메시지를 처리한다 (DLT 없음)")
    void retriesThreeTimesThenSkips() throws Exception {
        // Given — 201 은 Redis 장애처럼 항상 실패, 그 외는 성공
        willAnswer(invocation -> {
            QueueMetaRegisterCommand cmd = invocation.getArgument(0);
            calls.computeIfAbsent(cmd.dropId(), id -> new CopyOnWriteArrayList<>()).add(Instant.now());
            if (FAILING_DROP.equals(cmd.dropId())) {
                throw new QueryTimeoutException("redis down (테스트 주입)");
            }
            return null;
        }).given(registerService).register(any());

        // When — 파티션 1개라 202 는 201 의 재시도가 끝난 뒤에 처리된다
        publish(FAILING_DROP);
        publish(NEXT_DROP);

        // Then
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                .until(() -> !callsOf(NEXT_DROP).isEmpty());
        List<Instant> failing = callsOf(FAILING_DROP);
        assertThat(failing).hasSize(4);
        // 1초 간격 재시도: 첫 호출 ~ 마지막 재시도 사이가 3초 가까이 걸린다 (지터 여유 0.5초)
        assertThat(Duration.between(failing.getFirst(), failing.getLast()))
                .isGreaterThanOrEqualTo(Duration.ofMillis(2_500));
        assertThat(callsOf(NEXT_DROP)).hasSize(1);
    }
}
