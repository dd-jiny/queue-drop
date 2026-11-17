package com.queuedrop.ranking.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.ranking.domain.model.DropWindow;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 드로우 마감 시각 Redis 저장소 통합 테스트 (phase-7-ranking.md 3.3, 구현 결정 — 4장에 별도 ID 없음, T7-18·T7-30 경계로 분류).
 * 키: rank:drop:closeAt (Hash, field = dropId, value = epoch ms 문자열 — rank.lua 가 tonumber 로 읽는 형식).
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisDropWindowRepository.class})
class RedisDropWindowRepositoryTest {

    private static final String CLOSE_AT = "rank:drop:closeAt";
    private static final Instant CLOSE_1 = Instant.parse("2026-12-01T11:00:00Z");
    private static final Instant CLOSE_2 = Instant.parse("2026-12-02T09:30:00Z");

    @Autowired private RedisDropWindowRepository repository;
    @Autowired private StringRedisTemplate redis;
    @Autowired private LettuceConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
    }

    @Test
    @DisplayName("[T7-30] (경계) save 는 closeAt 을 epoch ms 문자열로 저장한다 (rank.lua 계약)")
    void savesEpochMillis() {
        // When
        repository.save(1L, CLOSE_1);

        // Then
        assertThat(redis.opsForHash().get(CLOSE_AT, "1")).isEqualTo(String.valueOf(CLOSE_1.toEpochMilli()));
    }

    @Test
    @DisplayName("[T7-30] (경계) findAll 은 저장된 id 는 closeAt 으로, 없는 id 는 closeAt=null 로 채운다")
    void findAllFillsMissingWithNull() {
        // Given
        repository.save(1L, CLOSE_1);
        repository.save(2L, CLOSE_2);

        // When
        Map<Long, DropWindow> windows = repository.findAll(List.of(1L, 3L, 2L));

        // Then
        assertThat(windows).containsExactlyInAnyOrderEntriesOf(Map.of(
                1L, new DropWindow(1L, CLOSE_1),
                2L, new DropWindow(2L, CLOSE_2),
                3L, new DropWindow(3L, null)));
    }

    @Test
    @DisplayName("[T7-30] (경계) closeAt 은 밀리초 정밀도로 왕복한다 (마이크로초는 버림)")
    void millisPrecision() {
        // Given
        Instant micros = Instant.parse("2026-12-01T11:00:00.123456Z");
        repository.save(1L, micros);

        // When
        DropWindow window = repository.findAll(List.of(1L)).get(1L);

        // Then
        assertThat(window.closeAt()).isEqualTo(micros.truncatedTo(ChronoUnit.MILLIS));
    }

    @Test
    @DisplayName("[T7-30] (경계) 재저장은 덮어쓰기 (drop.created 재전달 멱등)")
    void resaveOverwrites() {
        // Given
        repository.save(1L, CLOSE_1);

        // When
        repository.save(1L, CLOSE_1);
        repository.save(1L, CLOSE_2);

        // Then
        assertThat(redis.opsForHash().size(CLOSE_AT)).isEqualTo(1L);
        assertThat(repository.findAll(List.of(1L))).containsExactlyEntriesOf(Map.of(1L, new DropWindow(1L, CLOSE_2)));
    }

    @Test
    @DisplayName("[T7-30] (경계) 빈 목록은 HMGET 0인자 오류 없이 빈 맵, 중복 id 는 예외 없이 한 번만 담긴다")
    void emptyAndDuplicateInput() {
        // Given
        repository.save(1L, CLOSE_1);

        // When / Then
        assertThat(repository.findAll(List.of())).isEmpty();
        assertThat(repository.findAll(List.of(1L, 1L)))
                .containsExactlyEntriesOf(Map.of(1L, new DropWindow(1L, CLOSE_1)));
    }

    @Test
    @DisplayName("[T7-27] (경계, Redis) 잘못된 포트면 save·findAll 모두 RankingStoreUnavailableException(원인 DataAccessException)")
    void unreachableRedis() {
        // Given
        LettuceConnectionFactory badFactory = RedisTestSupport.unreachableFactory(connectionFactory.getHostName());
        try {
            RedisDropWindowRepository bad = new RedisDropWindowRepository(new StringRedisTemplate(badFactory));
            Map<String, Consumer<RedisDropWindowRepository>> calls = Map.of(
                    "save", r -> r.save(1L, CLOSE_1),
                    "findAll", r -> r.findAll(List.of(1L)));

            // When / Then
            RedisTestSupport.assertAllStoreUnavailable(bad, calls);
        } finally {
            badFactory.destroy();
        }
    }
}
