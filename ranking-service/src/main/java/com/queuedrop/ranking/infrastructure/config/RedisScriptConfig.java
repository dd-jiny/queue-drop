package com.queuedrop.ranking.infrastructure.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

/** 빈 이름 = RedisRankingRepository 필드 이름 (coupon 과 같은 형태) */
@Configuration
@EnableConfigurationProperties(RankingProperties.class)
public class RedisScriptConfig {

    @Bean
    RedisScript<Long> rankScript() {
        return RedisScript.of(new ClassPathResource("scripts/rank.lua"), Long.class);
    }
}
