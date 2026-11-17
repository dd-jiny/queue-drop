package com.queuedrop.entry.infrastructure.config;

import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

/** Spring Data Redis 는 EVALSHA 를 먼저 시도하고 NOSCRIPT 면 EVAL 로 재시도한다 */
@Configuration
public class RedisScriptConfig {

    @Bean
    RedisScript<Long> entryInitializeScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-initialize.lua"), Long.class);
    }

    @Bean
    @SuppressWarnings("rawtypes")
    RedisScript<List> entryApplyScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-apply.lua"), List.class);
    }

    @Bean
    RedisScript<Long> entryCompensateScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-compensate.lua"), Long.class);
    }

    @Bean
    RedisScript<Long> entryClaimScript() {
        return RedisScript.of(new ClassPathResource("scripts/entry-claim.lua"), Long.class);
    }
}
