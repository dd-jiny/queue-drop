package com.queuedrop.user.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.user.domain.exception.InvalidEmailException;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EmailTest {

    private static final String DOMAIN_PART = "@example.com";

    @Test
    @DisplayName("[T1-01] 앞뒤 공백과 대문자가 섞인 이메일은 공백 제거 후 소문자로 정규화된다")
    void normalizesTrimAndLowerCase() {
        // Given
        String raw = " User@Example.COM ";

        // When
        Email email = new Email(raw);

        // Then
        assertThat(email.value()).isEqualTo("user@example.com");
    }

    @Test
    @DisplayName("[T1-01] 정규화 결과가 같으면 같은 이메일로 취급된다")
    void sameAfterNormalizationIsEqual() {
        // Given / When
        Email upper = new Email("USER@EXAMPLE.COM");
        Email lower = new Email("user@example.com");

        // Then
        assertThat(upper).isEqualTo(lower);
    }

    static Stream<Arguments> invalidEmails() {
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("TLD 없음", "a@b"),
                Arguments.of("@ 없음", "no-at.com"),
                Arguments.of("256자", "a".repeat(256 - DOMAIN_PART.length()) + DOMAIN_PART));
    }

    @ParameterizedTest(name = "[T1-02] {0}")
    @MethodSource("invalidEmails")
    @DisplayName("[T1-02] 형식이 잘못됐거나 255자를 넘는 이메일은 InvalidEmailException")
    void rejectsInvalid(String description, String raw) {
        // Given: raw

        // When / Then
        assertThatThrownBy(() -> new Email(raw))
                .isInstanceOf(InvalidEmailException.class);
    }

    @Test
    @DisplayName("[T1-02] 정확히 255자인 이메일은 허용된다 (경계값)")
    void accepts255Chars() {
        // Given
        String raw = "a".repeat(255 - DOMAIN_PART.length()) + DOMAIN_PART;

        // When
        Email email = new Email(raw);

        // Then
        assertThat(email.value()).hasSize(255);
    }
}
