package com.queuedrop.queue.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.RedisContainerConfig;
import com.queuedrop.queue.infrastructure.config.RedisScriptConfig;
import com.queuedrop.queue.infrastructure.redis.support.EntryRemainingFixture;
import com.queuedrop.queue.infrastructure.redis.support.RedisTestSupport;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Import 구성은 RedisWaitingQueueRepositoryTest 와 같게 두어 컨텍스트(컨테이너)를 재사용한다 */
@DataRedisTest
@Import({RedisContainerConfig.class, RedisScriptConfig.class,
        RedisWaitingQueueRepository.class, RedisRemainingCapacityReader.class})
class RedisRemainingCapacityReaderTest {

    private static final Long DROP_ID = 1L;

    @Autowired
    private RedisRemainingCapacityReader reader;

    @Autowired
    private StringRedisTemplate redis;

    private EntryRemainingFixture entryRemaining;

    @BeforeEach
    void setUp() {
        RedisTestSupport.flushAll(redis);
        entryRemaining = new EntryRemainingFixture(redis);
    }

    @Test
    @DisplayName("[T3-06] entry 잔여 정원 키가 없으면 empty")
    void emptyWhenKeyMissing() {
        // When
        OptionalLong remaining = reader.read(DROP_ID);

        // Then
        assertThat(remaining).isEmpty();
    }

    @Test
    @DisplayName("[T3-05] entry 잔여 정원 키 \"5\" → 5, \"0\" → 0")
    void readsValue() {
        // Given
        entryRemaining.initialize(DROP_ID, 5);
        entryRemaining.soldOut(2L);

        // When / Then
        assertThat(reader.read(DROP_ID)).hasValue(5);
        assertThat(reader.read(2L)).hasValue(0);
    }
}
