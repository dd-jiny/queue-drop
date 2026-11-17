package com.queuedrop.ranking.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.RedisContainerConfig;
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
 * 표시 정보(제목·닉네임) Redis 저장소 통합 테스트 (phase-7-ranking.md 4장 T7-18).
 * 키: rank:drop:info / rank:user:info (Hash, field = id 문자열). Kafka 없이 Redis 슬라이스만 띄운다.
 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisDisplayNameRepository.class})
class RedisDisplayNameRepositoryTest {

    private static final String DROP_INFO = "rank:drop:info";
    private static final String USER_INFO = "rank:user:info";

    @Autowired private RedisDisplayNameRepository repository;
    @Autowired private StringRedisTemplate redis;
    @Autowired private LettuceConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
    }

    @Test
    @DisplayName("[T7-18] 제목 저장 후 일괄 조회, 없는 id 는 맵에서 제외")
    void dropTitles() {
        // Given
        repository.saveDropTitle(1L, "한정판 스니커즈");
        repository.saveDropTitle(2L, "Limited Hoodie");

        // When
        Map<Long, String> titles = repository.findDropTitles(List.of(1L, 2L, 999L));

        // Then
        assertThat(titles).containsExactlyInAnyOrderEntriesOf(Map.of(1L, "한정판 스니커즈", 2L, "Limited Hoodie"));
        assertThat(redis.opsForHash().get(DROP_INFO, "1")).isEqualTo("한정판 스니커즈");
    }

    @Test
    @DisplayName("[T7-18] 닉네임 저장 후 일괄 조회, 없는 id 는 맵에서 제외")
    void nicknames() {
        // Given
        repository.saveNickname(1001L, "원영");
        repository.saveNickname(1002L, "karina");

        // When
        Map<Long, String> names = repository.findNicknames(List.of(1002L, 1003L, 1001L));

        // Then
        assertThat(names).containsExactlyInAnyOrderEntriesOf(Map.of(1001L, "원영", 1002L, "karina"));
        assertThat(redis.opsForHash().get(USER_INFO, "1002")).isEqualTo("karina");
    }

    @Test
    @DisplayName("[T7-18] 같은 값 재저장은 멱등 (재전달), 다른 값이면 덮어쓴다")
    void resaveIdempotent() {
        // Given
        repository.saveDropTitle(1L, "제목");
        repository.saveNickname(1001L, "닉");

        // When — 같은 이벤트 재전달
        repository.saveDropTitle(1L, "제목");
        repository.saveNickname(1001L, "닉");

        // Then
        assertThat(redis.opsForHash().size(DROP_INFO)).isEqualTo(1L);
        assertThat(redis.opsForHash().size(USER_INFO)).isEqualTo(1L);
        assertThat(repository.findDropTitles(List.of(1L))).containsExactlyEntriesOf(Map.of(1L, "제목"));

        // When — 다른 값
        repository.saveDropTitle(1L, "바뀐 제목");

        // Then
        assertThat(repository.findDropTitles(List.of(1L))).containsExactlyEntriesOf(Map.of(1L, "바뀐 제목"));
    }

    @Test
    @DisplayName("[T7-18] (경계) 제목과 닉네임은 다른 Hash — 같은 id 라도 섞이지 않는다")
    void separateHashes() {
        // Given
        repository.saveDropTitle(7L, "드로우7");
        repository.saveNickname(7L, "유저7");

        // When / Then
        assertThat(repository.findDropTitles(List.of(7L))).containsExactlyEntriesOf(Map.of(7L, "드로우7"));
        assertThat(repository.findNicknames(List.of(7L))).containsExactlyEntriesOf(Map.of(7L, "유저7"));
    }

    @Test
    @DisplayName("[T7-18] (경계) 빈 목록 조회는 HMGET 0인자 오류 없이 빈 맵, 아무것도 없는 id 만 조회해도 빈 맵")
    void emptyInput() {
        // Given
        repository.saveDropTitle(1L, "제목");

        // When / Then
        assertThat(repository.findDropTitles(List.of())).isEmpty();
        assertThat(repository.findNicknames(List.of())).isEmpty();
        assertThat(repository.findDropTitles(List.of(999L))).isEmpty();
        assertThat(repository.findNicknames(List.of(999L))).isEmpty();
    }

    @Test
    @DisplayName("[T7-18] (경계) 조회 id 가 중복돼도 예외 없이 한 번만 담긴다")
    void duplicateIds() {
        // Given
        repository.saveDropTitle(1L, "제목");

        // When
        Map<Long, String> titles = repository.findDropTitles(List.of(1L, 1L, 2L));

        // Then
        assertThat(titles).containsExactlyEntriesOf(Map.of(1L, "제목"));
    }

    @Test
    @DisplayName("[T7-27] (경계, Redis) 잘못된 포트면 저장·조회 모두 RankingStoreUnavailableException(원인 DataAccessException)")
    void unreachableRedis() {
        // Given
        LettuceConnectionFactory badFactory = RedisTestSupport.unreachableFactory(connectionFactory.getHostName());
        try {
            RedisDisplayNameRepository bad = new RedisDisplayNameRepository(new StringRedisTemplate(badFactory));
            Map<String, Consumer<RedisDisplayNameRepository>> calls = Map.of(
                    "saveDropTitle", r -> r.saveDropTitle(1L, "제목"),
                    "saveNickname", r -> r.saveNickname(1L, "닉"),
                    "findDropTitles", r -> r.findDropTitles(List.of(1L)),
                    "findNicknames", r -> r.findNicknames(List.of(1L)));

            // When / Then
            RedisTestSupport.assertAllStoreUnavailable(bad, calls);
        } finally {
            badFactory.destroy();
        }
    }
}
