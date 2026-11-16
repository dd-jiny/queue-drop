package com.queuedrop.user.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.user.domain.exception.InvalidNicknameException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UserTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");
    private static final String HASH = "hashed-password";

    private static Email email() {
        return new Email("user@example.com");
    }

    @ParameterizedTest(name = "[T1-03] 닉네임 {0}")
    @ValueSource(strings = {"a", "abcdefghijklmnopqrstu", "   "})
    @DisplayName("[T1-03] 닉네임이 1자, 21자, 공백만이면 InvalidNicknameException")
    void rejectsInvalidNickname(String nickname) {
        // Given: nickname

        // When / Then
        assertThatThrownBy(() -> User.register(email(), HASH, nickname, NOW))
                .isInstanceOf(InvalidNicknameException.class);
    }

    @ParameterizedTest(name = "[T1-03] 닉네임 {0}")
    @ValueSource(strings = {"ab", "abcdefghijklmnopqrst"})
    @DisplayName("[T1-03] 닉네임이 2자, 20자이면 가입된다 (경계값)")
    void acceptsBoundaryNickname(String nickname) {
        // Given: nickname

        // When
        User user = User.register(email(), HASH, nickname, NOW);

        // Then
        assertThat(user.getNickname()).isEqualTo(nickname);
    }

    @Test
    @DisplayName("[T1-03] 닉네임 앞뒤 공백은 제거되어 저장된다")
    void trimsNickname() {
        // Given
        String nickname = "  drop  ";

        // When
        User user = User.register(email(), HASH, nickname, NOW);

        // Then
        assertThat(user.getNickname()).isEqualTo("drop");
    }

    @Test
    @DisplayName("[T1-03] 길이 판단은 공백 제거 후 기준이다 (공백 포함 22자, 제거 후 20자는 허용)")
    void lengthCheckedAfterTrim() {
        // Given
        String nickname = " " + "a".repeat(20) + " ";

        // When
        User user = User.register(email(), HASH, nickname, NOW);

        // Then
        assertThat(user.getNickname()).hasSize(20);
    }

    @Test
    @DisplayName("[T1-04] User.register 로 만든 회원은 role=USER, id=null 이고 입력값을 그대로 가진다")
    void registerCreatesUserRoleWithoutId() {
        // Given
        Email email = email();

        // When
        User user = User.register(email, HASH, "drop", NOW);

        // Then
        assertThat(user.getRole()).isEqualTo(Role.USER);
        assertThat(user.getId()).isNull();
        assertThat(user.getEmail()).isEqualTo(email);
        assertThat(user.getPasswordHash()).isEqualTo(HASH);
        assertThat(user.getCreatedAt()).isEqualTo(NOW);
    }
}
