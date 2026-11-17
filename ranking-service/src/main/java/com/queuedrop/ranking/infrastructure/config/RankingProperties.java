package com.queuedrop.ranking.infrastructure.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** ranking.* 프로퍼티 (phase-7-ranking.md 3.3). 기본값은 application.yml 에 둔다. */
@ConfigurationProperties(prefix = "ranking")
public record RankingProperties(
        Duration weeklyTtl,          // 기본 35d
        Duration dedupeGrace,        // 기본 7d
        Duration dedupeFallbackTtl,  // 기본 30d
        Duration dedupeMinTtl        // 기본 1d
) {}
