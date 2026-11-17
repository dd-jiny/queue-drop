package com.queuedrop.entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.Arrays;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * T0-03: 컨텍스트 기동 + Flyway 실행 + ddl-auto validate 확인.
 * Postgres 만 띄운다. Redis 는 Lettuce 지연 연결이라 기동 시 접속하지 않는다.
 * Kafka 리스너는 auto-startup=false 로 시작하지 않는다 — 켜 두면 기본 bootstrap(localhost:9092)인
 * 로컬 개발 Kafka 에 실제로 붙어 메시지를 소비·오프셋 커밋할 수 있다.
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@Import(PostgresContainerConfig.class)
@AutoConfigureMockMvc
class ApplicationContextTest {

    @Autowired
    Flyway flyway;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    EntityManagerFactory entityManagerFactory;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    @DisplayName("[T0-03] entry-service 컨텍스트가 기동되고 Flyway 가 실행되며 ddl-auto 는 validate 이다")
    void contextLoadsWithFlywayAndValidate() {
        // Given: PostgresContainerConfig 로 띄운 PostgreSQL 에 연결된 컨텍스트

        // When: 기동 시 Flyway migrate 가 실행된 결과를 조회
        Boolean historyTableExists = jdbcTemplate.queryForObject(
                "select exists (select 1 from information_schema.tables"
                        + " where table_schema = current_schema() and table_name = ?)",
                Boolean.class, flyway.getConfiguration().getTable());

        // Then: Flyway 가 대상 DB 에 실제로 적용됐고, 남은 마이그레이션이 없다
        assertThat(flyway.getConfiguration().getLocations())
                .extracting(Object::toString)
                .containsExactly("classpath:db/migration");
        assertThat(historyTableExists).isTrue();
        assertThat(flyway.info().pending()).isEmpty();

        // Then: Hibernate 스키마 전략은 validate (스키마는 Flyway 소유)
        assertThat(entityManagerFactory.getProperties())
                .containsEntry("hibernate.hbm2ddl.auto", "validate");
    }

    @Test
    @DisplayName("[T0-03] Postgres 전용 컨텍스트에서는 Kafka 리스너 컨테이너가 하나도 시작되지 않는다 (로컬 Kafka 소비 방지)")
    void kafkaListenersDoNotStart() {
        // Given: spring.kafka.listener.auto-startup=false

        // When / Then: 리스너는 등록돼 있지만 실행 중이 아니다
        assertThat(listenerRegistry.getListenerContainers()).isNotEmpty()
                .allSatisfy(container -> assertThat(container.isRunning()).isFalse());
    }

    // ---- 8단계 커밋 #2: Actuator 메트릭 노출 (phase-8 3.2) ----

    @Autowired
    MockMvc mockMvc;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    @DisplayName("[8단계 Actuator] entry-service 는 /actuator/prometheus 를 노출하고 application 태그와 http.server.requests 히스토그램 버킷을 내보낸다")
    void prometheusExposesApplicationTagAndHistogram() throws Exception {
        // Given: http.server.requests 는 요청이 한 번 이상 있어야 생기므로 먼저 한 번 호출한다
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());

        // When
        String body = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        // Then: 공통 태그 application=서비스명, percentiles-histogram 으로 버킷 시계열이 생긴다
        assertThat(body)
                .contains("application=\"entry-service\"")
                .contains("http_server_requests_seconds_bucket");
    }

    @Test
    @DisplayName("[8단계 Actuator] entry-service 의 http.server.requests 타이머에 application 태그와 0.5·0.95·0.99 퍼센타일이 설정된다")
    void httpServerRequestsHasApplicationTagAndPercentiles() throws Exception {
        // Given: 타이머 생성을 위해 요청 한 번
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());

        // When
        var timers = meterRegistry.get("http.server.requests").timers();

        // Then
        assertThat(timers).isNotEmpty().allSatisfy(timer -> {
            assertThat(timer.getId().getTag("application")).isEqualTo("entry-service");
            assertThat(Arrays.stream(timer.takeSnapshot().percentileValues()).mapToDouble(ValueAtPercentile::percentile))
                    .contains(0.5, 0.95, 0.99);
        });
    }

    @Test
    @DisplayName("[8단계 Actuator] entry-service 는 health 프로브(liveness·readiness)와 /actuator/metrics 를 노출한다")
    void healthProbesAndMetricsExposed() throws Exception {
        // When / Then: 프로브 그룹은 외부 의존 indicator 를 포함하지 않아 테스트 컨텍스트에서도 UP 이다
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/metrics")).andExpect(status().isOk());
    }
}
