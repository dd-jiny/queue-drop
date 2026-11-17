package com.queuedrop.gateway.auth;

import com.queuedrop.gateway.support.TestTokens;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.PathContainer;

import static org.assertj.core.api.Assertions.assertThat;

class PublicPathMatcherTest {

    private final PublicPathMatcher matcher = new PublicPathMatcher(TestTokens.authProperties());

    @ParameterizedTest(name = "{0} {1} → 공개={2}")
    @CsvSource({
            "GET,  /api/rankings/users/weekly,           true",
            "GET,  /api/rankings/users/me,               false",
            "POST, /api/products/drops/1/republish,      false",
            "GET,  /api/products/drops/1,                true",
            // 추가 경계: 메서드가 다르면 보호, 패턴 변수는 한 세그먼트만, 하위 경로는 보호
            "POST, /api/products/drops,                  false",
            "GET,  /api/users/signup,                    false",
            "POST, /api/users/signup,                    true",
            "GET,  /api/products/drops/1/extra,          false",
            "GET,  /api/entries/drops/7/result,          true",
            "GET,  /api/entries/drops/7,                 false",
            "GET,  /actuator/health,                     true",
            // 우회 시도 경계: 정확히 일치하지 않으면 보호 (fail-closed)
            "POST, /api/users/signup/,                   false",
            "POST, /API/users/signup,                    false",
            "GET,  /api/rankings/users/weekly/..;/me,    false",
            // 매트릭스 파라미터(;x)는 매칭 전에 제거되어 공개로 판정된다 (현재 동작 인정).
            // 다운스트림 Spring MVC 도 세미콜론 내용을 제거한 뒤 매핑하므로 같은 공개 리소스(GET drops/1)에만 닿고,
            // 공개 패턴에 ** 가 없어 다른 리소스로 번지지 않는다. 위의 ..; 조합은 세그먼트 수가 달라 보호로 판정된다.
            "GET,  /api/products/drops/1;x,              true"
    })
    @DisplayName("[T1-26] METHOD + 경로 패턴이 모두 일치할 때만 인증 제외 경로다")
    void matchesMethodAndPattern(String method, String path, boolean expected) {
        // When
        boolean result = matcher.isPublic(HttpMethod.valueOf(method), PathContainer.parsePath(path));

        // Then
        assertThat(result).isEqualTo(expected);
    }
}
