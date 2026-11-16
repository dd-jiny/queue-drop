package com.queuedrop.user;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.user.application.dto.CreateAdminCommand;
import com.queuedrop.user.application.service.UserService;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import com.queuedrop.user.domain.repository.UserRepository;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 여러 인스턴스 동시 기동 경쟁 재현: existsByEmail 사전 확인은 통과했지만 저장 시점에 이미 다른 인스턴스가
 * 같은 이메일을 저장해 유니크 위반이 나는 상황. 실제 PostgreSQL 과 실제 트랜잭션 경계로 확인한다.
 */
@SpringBootTest(properties = "queuedrop.admin.email=")   // 기동 러너는 끈다 (경쟁 데코레이터가 기동 경로에 끼지 않게)
@Import({PostgresContainerConfig.class, AdminBootstrapRaceIntegrationTest.RacingRepositoryConfig.class})
class AdminBootstrapRaceIntegrationTest {

    private static final String ADMIN_EMAIL = "race-admin@queuedrop.test";

    /** 실제 UserRepository 를 감싸 existsByEmail 만 항상 false 로 만든다 (사전 확인 통과 재현). */
    @TestConfiguration(proxyBeanMethods = false)
    static class RacingRepositoryConfig {
        @Bean
        @Primary
        UserRepository racingUserRepository(@Qualifier("userRepositoryImpl") UserRepository delegate) {
            return new UserRepository() {
                @Override public User save(User user) { return delegate.save(user); }
                @Override public Optional<User> findByEmail(Email email) { return delegate.findByEmail(email); }
                @Override public boolean existsByEmail(Email email) { return false; }
            };
        }
    }

    @Autowired UserService userService;
    @Autowired JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("[T1-28] 사전 확인 통과 후 저장 시 유니크 위반(동시 기동 경쟁)이면 실제 DB 에서도 예외 없이 false, ADMIN 은 1행")
    void concurrentBootstrapReturnsFalseWithoutRollbackError() {
        // Given: 다른 인스턴스가 먼저 관리자를 만든 상태
        CreateAdminCommand command = new CreateAdminCommand(ADMIN_EMAIL, "adminPassword1", "admin");
        assertThat(userService.createAdminIfAbsent(command)).isTrue();

        // When: 사전 확인은 통과(false)하고 저장에서 유니크 위반
        boolean created = userService.createAdminIfAbsent(command);

        // Then: UnexpectedRollbackException 등 없이 false, 행은 1개
        assertThat(created).isFalse();
        assertThat(jdbcTemplate.queryForObject(
                "select count(*) from users where email = ? and role = 'ADMIN'", Integer.class, ADMIN_EMAIL))
                .isEqualTo(1);
    }
}
