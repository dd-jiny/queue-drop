package com.queuedrop.entry.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.entry.application.config.DrawProperties;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class DrawPropertiesConfigTest {

    /** SpringApplication 과 같이 ApplicationConversionService 를 등록해 "5m"·"1d" 형식을 해석한다 */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(ctx -> ctx.getBeanFactory()
                    .setConversionService(ApplicationConversionService.getSharedInstance()))
            .withUserConfiguration(DrawPropertiesConfig.class);

    @Test
    @DisplayName("[T5-13·T5-30 보강] application.yml 형식(5m, 1d)이 DrawProperties 로 바인딩된다")
    void bindsDurations() {
        // Given / When
        runner.withPropertyValues(
                        "entry.draw.closing-warn-threshold=5m",
                        "entry.draw.key-ttl-after-draw=1d")
                // Then
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(DrawProperties.class))
                            .isEqualTo(new DrawProperties(Duration.ofMinutes(5), Duration.ofDays(1)));
                });
    }

    @ParameterizedTest(name = "closing-warn-threshold={0}, key-ttl-after-draw={1}")
    @CsvSource({
            "0s, 1d",
            "-1m, 1d",
            "5m, 0s",
            "5m, -1d"
    })
    @DisplayName("[T5-13·T5-30 보강] 0 이하 Duration 이면 기동 실패 (IllegalStateException)")
    void rejectsNonPositive(String threshold, String ttl) {
        // Given / When
        runner.withPropertyValues(
                        "entry.draw.closing-warn-threshold=" + threshold,
                        "entry.draw.key-ttl-after-draw=" + ttl)
                // Then
                .run(ctx -> assertThat(ctx).hasFailed()
                        .getFailure().rootCause().isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("0 보다 커야"));
    }
}
