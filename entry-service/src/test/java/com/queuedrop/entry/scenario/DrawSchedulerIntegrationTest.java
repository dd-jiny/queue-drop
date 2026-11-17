package com.queuedrop.entry.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.entry.application.dto.DrawCompletedPayload;
import com.queuedrop.entry.application.service.DrawService;
import com.queuedrop.entry.presentation.scheduler.DrawScheduler;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.config.FixedDelayTask;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.scheduling.config.ScheduledTaskHolder;

/**
 * 추첨 스케줄러 통합 (단계 문서 5단계 4장 T5-21, T5-22). 전체 entry 컨텍스트 (PostgreSQL + Redis + Kafka).
 * 자동 실행은 NoOpTaskScheduler 로 막고(ScenarioTestSupport) run() 을 직접 호출한다.
 */
class DrawSchedulerIntegrationTest extends ScenarioTestBase {

    @Autowired DrawScheduler drawScheduler;
    @Autowired DrawService drawService;
    @Autowired Clock clock;
    @Autowired ApplicationContext context;

    @Test
    @DisplayName("[T5-21] DrawScheduler.run 이 entry.draw.interval-ms(5000) 주기의 fixedDelay 작업으로 등록된다")
    void registeredAsFixedDelayTask() {
        // When
        List<ScheduledTask> tasks = context.getBeansOfType(ScheduledTaskHolder.class).values().stream()
                .flatMap(holder -> holder.getScheduledTasks().stream())
                .filter(t -> t.toString().contains(DrawScheduler.class.getName() + ".run"))
                .toList();

        // Then
        assertThat(tasks).as("@EnableScheduling + @Scheduled 로 등록된 DrawScheduler.run").hasSize(1);
        assertThat(tasks.getFirst().getTask()).isInstanceOf(FixedDelayTask.class);
        assertThat(((FixedDelayTask) tasks.getFirst().getTask()).getIntervalDuration()).isEqualTo(Duration.ofMillis(5000));
    }

    @Test
    @DisplayName("[T5-21] 응모 100건 저장·마감 지남, 스케줄러 2개 동시 실행 → draw_results 1건, DRAWN, WON 10/LOST 90, 발행된 추첨 결과 1건")
    void twoSchedulersDrawOnce() throws Exception {
        // Given — 정원 100·당첨 10, 마감 지남, Redis 응모자 100 = DB 저장 100
        long dropId = 501L;
        givenDueDrop(dropId, 100, 10);
        givenRedisApplicants(dropId, 1, 100);
        givenSavedEntries(dropId, 1, 100);
        DrawScheduler second = new DrawScheduler(drawService, clock);   // 다른 인스턴스 역할

        // When — 두 스케줄러를 동시에 출발
        Queue<Throwable> errors = runConcurrently(drawScheduler, second);

        // Then — 추첨 1회
        assertThat(errors).as("run() 은 예외를 밖으로 내지 않는다").isEmpty();
        assertThat(drawResultCount(dropId)).isEqualTo(1);
        Map<String, Object> result = jdbc.queryForMap(
                "SELECT applicant_count, winner_count, published FROM draw_results WHERE drop_id = ?", dropId);
        assertThat(result).containsEntry("applicant_count", 100).containsEntry("winner_count", 10)
                .containsEntry("published", true);
        assertThat(snapshotStatus(dropId)).isEqualTo("DRAWN");
        assertThat(entryCountByStatus(dropId, "WON")).isEqualTo(10);
        assertThat(entryCountByStatus(dropId, "LOST")).isEqualTo(90);
        assertThat(entryCountByStatus(dropId, "APPLIED")).isZero();

        // Then — Redis 마감 표시
        assertThat(redis.opsForHash().get("entry:" + dropId + ":meta", "state")).isEqualTo("CLOSED");

        // Then — 발행: 추첨 결과는 1건. 같은 결과의 재발행 중복은 허용 (단계 문서 6장 리스크 "draw.completed 중복 발행")
        List<DrawCompletedPayload> published = drawPublisher.published();
        assertThat(published).isNotEmpty();
        assertThat(Set.copyOf(published)).as("서로 다른 추첨 결과 발행은 1건").hasSize(1);
        DrawCompletedPayload payload = published.getFirst();
        assertThat(payload.dropId()).isEqualTo(dropId);
        assertThat(payload.applicantCount()).isEqualTo(100);
        assertThat(payload.winnerCount()).isEqualTo(10);
        assertThat(payload.drawnAt()).isEqualTo(ScenarioTestSupport.NOW);
    }

    @Test
    @DisplayName("[T5-22] Redis 응모자 100 / DB 90 이면 1회차는 추첨하지 않고, 나머지 10건 저장 후 2회차에 100명 기준으로 추첨한다")
    void waitsUntilAllEntriesSaved() {
        // Given — 응모 100건 중 90건만 저장됨
        long dropId = 502L;
        givenDueDrop(dropId, 100, 10);
        givenRedisApplicants(dropId, 1, 100);
        givenSavedEntries(dropId, 1, 90);

        // When — 1회차
        drawScheduler.run();

        // Then — 마감 잠금만 되고 추첨 없음
        assertThat(snapshotStatus(dropId)).isEqualTo("CLOSING");
        assertThat(drawResultCount(dropId)).isZero();
        assertThat(entryCountByStatus(dropId, "APPLIED")).isEqualTo(90);
        assertThat(drawPublisher.published()).isEmpty();

        // When — 나머지 10건 저장 후 2회차
        givenSavedEntries(dropId, 91, 100);
        drawScheduler.run();

        // Then — 100명 기준 추첨, 발행 1건
        assertThat(snapshotStatus(dropId)).isEqualTo("DRAWN");
        assertThat(drawResultCount(dropId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT applicant_count FROM draw_results WHERE drop_id = ?", Integer.class, dropId))
                .isEqualTo(100);
        assertThat(entryCountByStatus(dropId, "WON")).isEqualTo(10);
        assertThat(entryCountByStatus(dropId, "LOST")).isEqualTo(90);
        assertThat(drawPublisher.published()).hasSize(1)
                .first().extracting(DrawCompletedPayload::applicantCount).isEqualTo(100);
    }

    /** 스케줄러들을 동시에 출발시키고 끝까지 기다린다. 던져진 예외를 모아 돌려준다 */
    private Queue<Throwable> runConcurrently(DrawScheduler... schedulers) throws InterruptedException {
        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(schedulers.length);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(schedulers.length);
        try (ExecutorService pool = Executors.newFixedThreadPool(schedulers.length)) {
            for (DrawScheduler scheduler : schedulers) {
                pool.submit(() -> {
                    try {
                        ready.countDown();
                        start.await();
                        scheduler.run();
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).as("두 스케줄러가 60초 안에 끝났다").isTrue();
        }
        return errors;
    }
}
