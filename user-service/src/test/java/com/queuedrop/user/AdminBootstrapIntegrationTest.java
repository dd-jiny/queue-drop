package com.queuedrop.user;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = {
        "queuedrop.admin.email=" + AdminBootstrapIntegrationTest.ADMIN_EMAIL,
        "queuedrop.admin.password=" + AdminBootstrapIntegrationTest.ADMIN_PASSWORD,
        "queuedrop.admin.nickname=admin"
})
@AutoConfigureMockMvc
@Import(PostgresContainerConfig.class)
class AdminBootstrapIntegrationTest {

    static final String ADMIN_EMAIL = "admin@queuedrop.test";
    static final String ADMIN_PASSWORD = "adminPassword1";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired PostgreSQLContainer postgres;
    @Value("${queuedrop.jwt.secret}") String jwtSecret;

    private int countAdmins() {
        return jdbcTemplate.queryForObject(
                "select count(*) from users where email = ? and role = 'ADMIN'", Integer.class, ADMIN_EMAIL);
    }

    @Test
    @DisplayName("[T1-31] 기동 시 관리자 계정이 만들어져 로그인하면 role=ADMIN 토큰이 발급되고, 같은 DB 로 재기동해도 ADMIN 은 1행이다")
    void adminCreatedOnBootstrapAndIdempotentOnRestart() throws Exception {
        // Given: ADMIN_EMAIL/PASSWORD 를 지정해 기동된 컨텍스트

        // When: 관리자 계정으로 로그인
        MvcResult login = mockMvc.perform(post("/api/users/login").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"%s","password":"%s"}""".formatted(ADMIN_EMAIL, ADMIN_PASSWORD)))
                .andReturn();

        // Then: 토큰 role claim = ADMIN, users 에 ADMIN 1행
        assertThat(login.getResponse().getStatus()).isEqualTo(200);
        String token = JsonMapper.builder().build()
                .readTree(login.getResponse().getContentAsString()).path("accessToken").asString();
        Claims claims = Jwts.parser()
                .verifyWith(Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8)))
                .build().parseSignedClaims(token).getPayload();
        assertThat(claims.get("role", String.class)).isEqualTo("ADMIN");
        assertThat(countAdmins()).isEqualTo(1);

        // When: 같은 DB 를 바라보는 애플리케이션을 한 번 더 기동 (재배포·재시작 재현)
        try (ConfigurableApplicationContext restarted = new SpringApplicationBuilder(UserServiceApplication.class)
                .properties(
                        "spring.main.web-application-type=none",
                        "spring.datasource.url=" + postgres.getJdbcUrl(),
                        "spring.datasource.username=" + postgres.getUsername(),
                        "spring.datasource.password=" + postgres.getPassword(),
                        "queuedrop.admin.email=" + ADMIN_EMAIL,
                        "queuedrop.admin.password=" + ADMIN_PASSWORD,
                        "queuedrop.admin.nickname=admin")
                .run()) {
            // Then: 재기동이 실패하지 않는다
            assertThat(restarted.isActive()).isTrue();
        }

        // Then: 여전히 1행
        assertThat(countAdmins()).isEqualTo(1);
    }
}
