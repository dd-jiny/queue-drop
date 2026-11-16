package com.queuedrop.queue.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

/** E-36: saveMeta 를 HSET + PEXPIREAT + ZADD 원자 Lua 하나로 처리한다 */
class RedisScriptConfigTest {

    @Test
    @DisplayName("[T3-25] saveMetaScript 빈은 scripts/save_meta.lua 를 읽고 Long 을 반환한다")
    void saveMetaScript() {
        // Given
        RedisScriptConfig config = new RedisScriptConfig();

        // When
        RedisScript<Long> script = config.saveMetaScript();

        // Then
        assertThat(new ClassPathResource("scripts/save_meta.lua").exists()).isTrue();
        assertThat(script.getResultType()).isEqualTo(Long.class);
        assertThat(script.getScriptAsString()).contains("HSET").contains("PEXPIREAT").contains("ZADD");
    }
}
