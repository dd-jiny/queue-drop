package com.queuedrop.user.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.user.domain.exception.DuplicateEmailException;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.Role;
import com.queuedrop.user.domain.model.User;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresContainerConfig.class, UserRepositoryImpl.class, UserMapper.class})
class UserRepositoryImplTest {

    /** PostgreSQL timestamptz 는 마이크로초 정밀도. 마이크로초 단위까지 왕복되는지 본다. */
    private static final Instant CREATED_AT = Instant.parse("2026-11-16T10:00:00.123456Z");

    @Autowired UserRepositoryImpl userRepository;
    @Autowired TestEntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired EntityManagerFactory entityManagerFactory;

    private static User newUser(String email) {
        return User.register(new Email(email), "$2a$10$hash", "drop", CREATED_AT);
    }

    @Test
    @DisplayName("[T1-10] 슬라이스 기동 시 Flyway V1 이 적용되고 ddl-auto 는 validate 이다")
    void flywayAppliedAndSchemaValidated() {
        // Given: @DataJpaTest + PostgresContainerConfig 로 기동된 슬라이스

        // When
        String appliedVersion = jdbcTemplate.queryForObject(
                "select version from flyway_schema_history where success order by installed_rank desc limit 1",
                String.class);

        // Then
        assertThat(appliedVersion).isEqualTo("1");
        assertThat(entityManagerFactory.getProperties()).containsEntry("hibernate.hbm2ddl.auto", "validate");
    }

    @Test
    @DisplayName("[T1-10] save 후 findByEmail 로 다시 읽으면 id 가 부여되고 모든 필드(마이크로초 Instant 포함)가 일치한다")
    void saveAndFindRoundTrip() {
        // Given
        User saved = userRepository.save(newUser("user@example.com"));
        entityManager.clear();   // 1차 캐시가 아니라 DB 에서 다시 읽게 한다

        // When
        User found = userRepository.findByEmail(new Email("user@example.com")).orElseThrow();

        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(found.getId()).isEqualTo(saved.getId());
        assertThat(found.getEmail()).isEqualTo(new Email("user@example.com"));
        assertThat(found.getPasswordHash()).isEqualTo("$2a$10$hash");
        assertThat(found.getNickname()).isEqualTo("drop");
        assertThat(found.getRole()).isEqualTo(Role.USER);
        assertThat(found.getCreatedAt()).isEqualTo(CREATED_AT);
    }

    @Test
    @DisplayName("[T1-10] existsByEmail 은 저장된 이메일에만 true, findByEmail 은 없는 이메일에 빈 값")
    void existsAndFindMissing() {
        // Given
        userRepository.save(newUser("user@example.com"));
        entityManager.clear();

        // When / Then
        assertThat(userRepository.existsByEmail(new Email("user@example.com"))).isTrue();
        assertThat(userRepository.existsByEmail(new Email("other@example.com"))).isFalse();
        assertThat(userRepository.findByEmail(new Email("other@example.com"))).isEmpty();
    }

    @Test
    @DisplayName("[T1-11] 같은 이메일이 이미 저장돼 있으면 save 는 DuplicateEmailException 을 던진다")
    void duplicateEmailTranslated() {
        // Given
        userRepository.save(newUser("user@example.com"));
        entityManager.clear();

        // When / Then
        assertThatThrownBy(() -> userRepository.save(newUser("user@example.com")))
                .isInstanceOf(DuplicateEmailException.class);
    }
}
