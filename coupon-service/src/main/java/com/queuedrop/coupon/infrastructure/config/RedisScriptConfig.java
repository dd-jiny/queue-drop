package com.queuedrop.coupon.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

/** Spring Data Redis 는 EVALSHA 를 먼저 시도하고 NOSCRIPT 면 EVAL 로 재시도한다. 빈 이름 = RedisCouponGate 필드 이름 */
@Configuration
public class RedisScriptConfig {

    @Bean
    RedisScript<String> couponIssueScript() {
        return RedisScript.of(new ClassPathResource("scripts/coupon-issue.lua"), String.class);
    }

    @Bean
    RedisScript<Long> couponCompensateScript() {
        return RedisScript.of(new ClassPathResource("scripts/coupon-compensate.lua"), Long.class);
    }

    @Bean
    RedisScript<Long> couponClaimScript() {
        return RedisScript.of(new ClassPathResource("scripts/coupon-claim.lua"), Long.class);
    }
}
