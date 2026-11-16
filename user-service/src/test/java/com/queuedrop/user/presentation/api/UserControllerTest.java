package com.queuedrop.user.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.user.application.dto.SignupCommand;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * UserService 는 구체 클래스라 Mockito 대신 Fake 포트로 조립한 실제 UserService 를 빈으로 넣는다.
 * presentation 테스트이므로 domain 타입은 import 하지 않는다 (Given 도 application API 로 만든다).
 */
@WebMvcTest(UserController.class)
class UserControllerTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");

    @TestConfiguration
    static class FakeUserServiceConfig {
        @Bean InMemoryUserRepository userRepository() { return new InMemoryUserRepository(); }
        @Bean FakeTokenIssuer tokenIssuer() { return new FakeTokenIssuer(); }
        @Bean RecordingUserEventPublisher eventPublisher() { return new RecordingUserEventPublisher(); }

        @Bean
        UserService userService(InMemoryUserRepository repository, FakeTokenIssuer tokenIssuer,
                                RecordingUserEventPublisher publisher) {
            return new UserService(repository, new FakePasswordEncoder(), tokenIssuer, publisher,
                    Clock.fixed(NOW, ZoneOffset.UTC));
        }
    }

    @Autowired MockMvc mockMvc;
    @Autowired UserService userService;
    @Autowired InMemoryUserRepository userRepository;
    @Autowired FakeTokenIssuer tokenIssuer;
    @Autowired RecordingUserEventPublisher eventPublisher;

    @BeforeEach
    void resetFakes() {
        userRepository.clear();
        tokenIssuer.clear();
        eventPublisher.clear();
    }

    private ResultActions postJson(String path, String body) throws Exception {
        return mockMvc.perform(post(path).contentType(MediaType.APPLICATION_JSON).content(body));
    }

    @Test
    @DisplayName("[T1-14] 비밀번호 7자로 가입하면 400 COMMON_INVALID_INPUT 과 password fieldError, 서비스는 실행되지 않는다")
    void signupWithShortPassword() throws Exception {
        // Given
        String body = """
                {"email":"user@example.com","password":"1234567","nickname":"drop"}""";

        // When
        ResultActions result = postJson("/api/users/signup", body);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("password")));
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-14] 이메일 형식이 잘못되면 400 COMMON_INVALID_INPUT 과 email fieldError")
    void signupWithInvalidEmail() throws Exception {
        // Given
        String body = """
                {"email":"not-an-email","password":"password123","nickname":"drop"}""";

        // When
        ResultActions result = postJson("/api/users/signup", body);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("email")));
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-15] 이미 가입된 이메일이면 서비스의 DUPLICATE_EMAIL 이 409 USER_DUPLICATE_EMAIL 로 매핑된다")
    void signupDuplicateEmail() throws Exception {
        // Given
        userService.signup(new SignupCommand("user@example.com", "password123", "drop"));
        String body = """
                {"email":"user@example.com","password":"password123","nickname":"other"}""";

        // When
        ResultActions result = postJson("/api/users/signup", body);

        // Then
        result.andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("USER_DUPLICATE_EMAIL"))
                .andExpect(jsonPath("$.message").value("이미 가입된 이메일입니다."));
    }

    @Test
    @DisplayName("[T1-15] 정상 가입은 201 과 {userId, email, nickname} 바디를 돌려준다")
    void signupCreated() throws Exception {
        // Given
        String body = """
                {"email":"User@Example.com","password":"password123","nickname":"drop"}""";

        // When
        ResultActions result = postJson("/api/users/signup", body);

        // Then
        result.andExpect(status().isCreated())
                .andExpect(jsonPath("$.userId").value(1))
                .andExpect(jsonPath("$.email").value("user@example.com"))
                .andExpect(jsonPath("$.nickname").value("drop"))
                .andExpect(jsonPath("$.password").doesNotExist());
    }

    @Test
    @DisplayName("[T1-15] 정상 로그인은 200 과 {accessToken, tokenType=Bearer, expiresIn} 바디를 돌려준다")
    void loginOk() throws Exception {
        // Given
        userService.signup(new SignupCommand("user@example.com", "password123", "drop"));
        String body = """
                {"email":"user@example.com","password":"password123"}""";

        // When
        ResultActions result = postJson("/api/users/login", body);

        // Then
        result.andExpect(status().isOk())
                .andExpect(jsonPath("$.accessToken").value("token-for-1"))
                .andExpect(jsonPath("$.tokenType").value("Bearer"))
                .andExpect(jsonPath("$.expiresIn").value(3600));
    }

    @Test
    @DisplayName("[T1-15] 비밀번호가 틀리면 401 USER_INVALID_CREDENTIALS 로 매핑된다")
    void loginInvalidCredentials() throws Exception {
        // Given
        userService.signup(new SignupCommand("user@example.com", "password123", "drop"));
        String body = """
                {"email":"user@example.com","password":"wrongPassword1"}""";

        // When
        ResultActions result = postJson("/api/users/login", body);

        // Then
        result.andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("USER_INVALID_CREDENTIALS"));
        assertThat(tokenIssuer.issuedSubjects()).isEmpty();
    }

    private static final String HANGUL_24 = "가".repeat(24);   // UTF-8 3바이트 x 24 = 72바이트

    private static String signupBody(String password) {
        return """
                {"email":"user@example.com","password":"%s","nickname":"drop"}""".formatted(password);
    }

    @Test
    @DisplayName("[T1-14] ASCII 64자 비밀번호(64바이트)는 201")
    void signupAscii64Password() throws Exception {
        // Given
        String password = "a".repeat(64);

        // When / Then
        postJson("/api/users/signup", signupBody(password)).andExpect(status().isCreated());
    }

    @Test
    @DisplayName("[T1-14] 한글 24자 비밀번호(정확히 72바이트, BCrypt 한계)는 201")
    void signupExactly72BytesPassword() throws Exception {
        // When / Then
        postJson("/api/users/signup", signupBody(HANGUL_24)).andExpect(status().isCreated());
    }

    @Test
    @DisplayName("[T1-14] 한글 24자 + ASCII 1자(73바이트)는 400 COMMON_INVALID_INPUT + password fieldError")
    void signup73BytesPasswordRejected() throws Exception {
        // When
        ResultActions result = postJson("/api/users/signup", signupBody(HANGUL_24 + "a"));

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("password")));
        assertThat(userRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("[T1-14] 한글 25자 비밀번호(25자지만 75바이트)는 400 COMMON_INVALID_INPUT + password fieldError")
    void signup75BytesPasswordRejected() throws Exception {
        // When
        ResultActions result = postJson("/api/users/signup", signupBody("가".repeat(25)));

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("password")));
        assertThat(userRepository.findAll()).isEmpty();
    }
}
