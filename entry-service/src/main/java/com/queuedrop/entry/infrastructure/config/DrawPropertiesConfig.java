package com.queuedrop.entry.infrastructure.config;

import com.queuedrop.entry.application.config.DrawProperties;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** entry.draw.* 바인딩·검증. DrawProperties 는 application 의 순수 값 객체 (queue 의 AdmissionProperties 와 같은 방식) */
@Configuration
public class DrawPropertiesConfig {

    @Bean
    DrawProperties drawProperties(
            @Value("${entry.draw.closing-warn-threshold}") Duration closingWarnThreshold,
            @Value("${entry.draw.key-ttl-after-draw}") Duration keyTtlAfterDraw) {
        if (closingWarnThreshold.isNegative() || closingWarnThreshold.isZero()
                || keyTtlAfterDraw.isNegative() || keyTtlAfterDraw.isZero()) {
            throw new IllegalStateException("entry.draw.closing-warn-threshold·key-ttl-after-draw 는 0 보다 커야 합니다");
        }
        return new DrawProperties(closingWarnThreshold, keyTtlAfterDraw);
    }
}
