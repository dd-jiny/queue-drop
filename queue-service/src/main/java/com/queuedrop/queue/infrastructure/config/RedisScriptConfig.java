package com.queuedrop.queue.infrastructure.config;

import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

/** Spring Data Redis 는 EVALSHA 를 먼저 시도하고 NOSCRIPT 면 EVAL 로 재시도한다 */
@Configuration
public class RedisScriptConfig {

    @Bean
    RedisScript<Long> enqueueScript() {
        return RedisScript.of(new ClassPathResource("scripts/enqueue.lua"), Long.class);
    }

    @Bean
    RedisScript<Long> saveMetaScript() {
        return RedisScript.of(new ClassPathResource("scripts/save_meta.lua"), Long.class);
    }

    @Bean
    @SuppressWarnings("rawtypes")
    RedisScript<List> admitScript() {
        return RedisScript.of(new ClassPathResource("scripts/admit.lua"), List.class);
    }
}
