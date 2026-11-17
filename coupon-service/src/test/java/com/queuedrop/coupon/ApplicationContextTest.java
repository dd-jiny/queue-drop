package com.queuedrop.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * T0-03: 컨텍스트 기동 + Flyway 실행 + ddl-auto validate 확인.
 * Redis 는 Lettuce 지연 연결이라 기동 시 접속하지 않으므로 Postgres 만 띄운다.
 * Kafka 리스너(CouponIssuedListener, 커밋 #5)는 auto-startup=false 로 시작하지 않는다 — 켜 두면 기본
 * bootstrap(localhost:9092)인 로컬 개발 Kafka 에 실제로 붙어 소비·오프셋 커밋할 수 있다 (entry 와 같음).
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@Import(PostgresContainerConfig.class)
class ApplicationContextTest {

    @Autowired
    Flyway flyway;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    EntityManagerFactory entityManagerFactory;

    @Test
    @DisplayName("[T0-03] coupon-service 컨텍스트가 기동되고 Flyway 가 실행되며 ddl-auto 는 validate 이다")
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
}
