package com.queuedrop.user.presentation.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.user.application.dto.LoginCommand;
import com.queuedrop.user.application.port.TokenSubject;
import com.queuedrop.user.application.service.UserService;
import com.queuedrop.user.application.service.fake.FakePasswordEncoder;
import com.queuedrop.user.application.service.fake.FakeTokenIssuer;
import com.queuedrop.user.application.service.fake.InMemoryUserRepository;
import com.queuedrop.user.application.service.fake.RecordingUserEventPublisher;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.DefaultApplicationArguments;

/**
 * Spring 없이 러너를 직접 실행한다. UserService 는 Fake 포트로 조립한 실제 객체이고,
 * "호출 여부"는 저장소 상태로 확인한다. presentation 테스트이므로 domain 타입은 import 하지 않는다.
 */
class AdminAccountInitializerTest {

    private InMemoryUserRepository userRepository;
    private FakeTokenIssuer tokenIssuer;
    private UserService userService;

    @BeforeEach
    void setUp() {
        userRepository = new InMemoryUserRepository();
        tokenIssuer = new FakeTokenIssuer();
        userService = new UserService(userRepository, new FakePasswordEncoder(), tokenIssuer,
                new RecordingUserEventPublisher(), Clock.fixed(Instant.parse("2026-11-16T10:00:00Z"), ZoneOffset.UTC));
    }

    private void run(AdminAccountProperties properties) {
        new AdminAccountInitializer(properties, userService).run(new DefaultApplicationArguments());
    }

    @ParameterizedTest(name = "[T1-30] email=\"{0}\"")
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    @DisplayName("[T1-30] ADMIN_EMAIL 이 비어 있으면 관리자 생성을 건너뛴다 (비밀번호가 없어도 기동 실패하지 않음)")
    void skipsWhenEmailBlank(String email) {
        // Given
        AdminAccountProperties properties = new AdminAccountProperties(email, "", "admin");

        // When
        run(properties);

        // Then
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-30] ADMIN_PASSWORD 가 7자면 IllegalStateException 으로 기동을 멈추고 계정을 만들지 않는다")
    void failsWhenPasswordTooShort() {
        // Given
        AdminAccountProperties properties = new AdminAccountProperties("admin@example.com", "1234567", "admin");

        // When / Then
        assertThatThrownBy(() -> run(properties)).isInstanceOf(IllegalStateException.class);
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-30] ADMIN_PASSWORD 가 null 이면 IllegalStateException")
    void failsWhenPasswordMissing() {
        // Given
        AdminAccountProperties properties = new AdminAccountProperties("admin@example.com", null, "admin");

        // When / Then
        assertThatThrownBy(() -> run(properties)).isInstanceOf(IllegalStateException.class);
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-30] 설정이 유효하면 관리자 계정이 생성되고, 다시 실행해도 1건이며 그 계정으로 ADMIN 토큰이 발급된다")
    void createsAdminOnceWhenConfigured() {
        // Given: 비밀번호 8자 (경계값)
        AdminAccountProperties properties = new AdminAccountProperties("admin@example.com", "12345678", "admin");

        // When
        run(properties);
        run(properties);

        // Then
        assertThat(userRepository.findAll()).hasSize(1);
        userService.login(new LoginCommand("admin@example.com", "12345678"));
        assertThat(tokenIssuer.issuedSubjects()).extracting(TokenSubject::role).containsExactly("ADMIN");
    }

    @Test
    @DisplayName("[T1-30] ADMIN_PASSWORD 가 UTF-8 73바이트면 IllegalStateException (BCrypt 72바이트 한계)")
    void failsWhenPasswordOver72Bytes() {
        // Given: 한글 24자(72바이트) + ASCII 1자
        AdminAccountProperties properties = new AdminAccountProperties("admin@example.com", "가".repeat(24) + "a", "admin");

        // When / Then
        assertThatThrownBy(() -> run(properties)).isInstanceOf(IllegalStateException.class);
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-30] ADMIN_PASSWORD 가 정확히 72바이트면 관리자 계정이 생성된다 (경계값)")
    void createsAdminWithExactly72Bytes() {
        // Given
        AdminAccountProperties properties = new AdminAccountProperties("admin@example.com", "가".repeat(24), "admin");

        // When
        run(properties);

        // Then
        assertThat(userRepository.findAll()).hasSize(1);
    }
}
