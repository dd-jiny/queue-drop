package com.queuedrop.user.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.user.application.dto.LoginCommand;
import com.queuedrop.user.application.dto.LoginResult;
import com.queuedrop.user.application.dto.SignupCommand;
import com.queuedrop.user.application.dto.SignupResult;
import com.queuedrop.user.application.exception.UserErrorCode;
import com.queuedrop.user.application.port.TokenSubject;
import com.queuedrop.user.application.port.UserRegisteredPayload;
import com.queuedrop.user.application.service.fake.FakePasswordEncoder;
import com.queuedrop.user.application.service.fake.FakeTokenIssuer;
import com.queuedrop.user.application.service.fake.InMemoryUserRepository;
import com.queuedrop.user.application.service.fake.RecordingUserEventPublisher;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UserServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");
    private static final String EMAIL = "user@example.com";
    private static final String PASSWORD = "password123";
    private static final String NICKNAME = "drop";

    private InMemoryUserRepository userRepository;
    private FakePasswordEncoder passwordEncoder;
    private FakeTokenIssuer tokenIssuer;
    private RecordingUserEventPublisher eventPublisher;
    private UserService userService;

    @BeforeEach
    void setUp() {
        userRepository = new InMemoryUserRepository();
        passwordEncoder = new FakePasswordEncoder();
        tokenIssuer = new FakeTokenIssuer();
        eventPublisher = new RecordingUserEventPublisher();
        userService = new UserService(userRepository, passwordEncoder, tokenIssuer, eventPublisher,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    /** 서비스를 거치지 않고 저장소에 직접 가입자를 둔다 (Given 이 검증 대상에 의존하지 않도록). */
    private User givenRegisteredUser(String email, String rawPassword) {
        return userRepository.save(User.register(new Email(email), passwordEncoder.encode(rawPassword), NICKNAME, NOW));
    }

    @Test
    @DisplayName("[T1-05] 빈 저장소에 가입하면 해시된 비밀번호로 저장되고 이벤트 1건이 발행되며 결과에 userId 가 담긴다")
    void signupStoresHashedPasswordAndPublishesEvent() {
        // Given: 빈 저장소
        SignupCommand command = new SignupCommand(EMAIL, PASSWORD, NICKNAME);

        // When
        SignupResult result = userService.signup(command);

        // Then: 저장 상태
        assertThat(userRepository.findAll()).hasSize(1);
        User saved = userRepository.findAll().getFirst();
        assertThat(saved.getPasswordHash()).isNotEqualTo(PASSWORD);
        assertThat(passwordEncoder.matches(PASSWORD, saved.getPasswordHash())).isTrue();
        assertThat(saved.getCreatedAt()).isEqualTo(NOW);

        // Then: 결과
        assertThat(result.userId()).isNotNull().isEqualTo(saved.getId());
        assertThat(result.email()).isEqualTo(EMAIL);
        assertThat(result.nickname()).isEqualTo(NICKNAME);

        // Then: 이벤트
        assertThat(eventPublisher.registered())
                .containsExactly(new UserRegisteredPayload(saved.getId(), NICKNAME));
    }

    @Test
    @DisplayName("[T1-06] 대소문자만 다른 같은 이메일이 이미 가입돼 있으면 USER_DUPLICATE_EMAIL 이고 이벤트는 발행되지 않는다")
    void signupWithCaseInsensitiveDuplicateEmailFails() {
        // Given
        givenRegisteredUser(EMAIL, PASSWORD);
        SignupCommand command = new SignupCommand("User@Example.COM", "otherPassword1", "other");

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> userService.signup(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(UserErrorCode.DUPLICATE_EMAIL));
        assertThat(userRepository.findAll()).hasSize(1);
        assertThat(eventPublisher.registered()).isEmpty();
    }

    @Test
    @DisplayName("[T1-07] exists 확인 후 저장 시점에 DuplicateEmailException 이 나면(동시 가입) USER_DUPLICATE_EMAIL 로 번역된다")
    void signupTranslatesConcurrentDuplicateOnSave() {
        // Given
        userRepository.throwDuplicateOnSave();
        SignupCommand command = new SignupCommand(EMAIL, PASSWORD, NICKNAME);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> userService.signup(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(UserErrorCode.DUPLICATE_EMAIL));
        assertThat(eventPublisher.registered()).isEmpty();
    }

    @Test
    @DisplayName("[T1-08] 올바른 비밀번호로 로그인하면 (userId, USER) 로 토큰이 발급되고 tokenType 은 Bearer 이다")
    void loginIssuesTokenForUser() {
        // Given
        User registered = givenRegisteredUser(EMAIL, PASSWORD);

        // When
        LoginResult result = userService.login(new LoginCommand(EMAIL, PASSWORD));

        // Then
        assertThat(tokenIssuer.issuedSubjects())
                .containsExactly(new TokenSubject(registered.getId(), "USER"));
        assertThat(result.tokenType()).isEqualTo("Bearer");
        assertThat(result.accessToken()).isEqualTo("token-for-" + registered.getId());
        assertThat(result.expiresInSeconds()).isEqualTo(FakeTokenIssuer.EXPIRES_IN_SECONDS);
    }

    @ParameterizedTest(name = "[T1-09] {0}")
    @CsvSource({
            "비밀번호 불일치, user@example.com, wrongPassword1",
            "없는 이메일, nobody@example.com, password123",
            "형식 오류 이메일, not-an-email, password123"
    })
    @DisplayName("[T1-09] 비밀번호 불일치·없는 이메일·형식 오류 이메일은 모두 USER_INVALID_CREDENTIALS 이고 토큰은 발급되지 않는다")
    void loginFailuresShareInvalidCredentials(String description, String email, String rawPassword) {
        // Given
        givenRegisteredUser(EMAIL, PASSWORD);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> userService.login(new LoginCommand(email, rawPassword)))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(UserErrorCode.INVALID_CREDENTIALS));
        assertThat(tokenIssuer.issuedSubjects()).isEmpty();
    }
}
