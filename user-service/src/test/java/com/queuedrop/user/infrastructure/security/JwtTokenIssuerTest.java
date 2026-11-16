package com.queuedrop.user.infrastructure.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.queuedrop.user.application.port.IssuedToken;
import com.queuedrop.user.application.port.TokenSubject;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.WeakKeyException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JwtTokenIssuerTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");
    private static final String SECRET = "0123456789abcdef0123456789abcdef";   // 32바이트
    private static final String ISSUER = "queue-drop";
    private static final Duration TTL = Duration.ofHours(1);

    private static JwtTokenIssuer issuer(String secret) {
        return new JwtTokenIssuer(new JwtProperties(secret, ISSUER, TTL), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static Jws<Claims> parse(String token) {
        SecretKey key = Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8));
        return Jwts.parser()
                .verifyWith(key)
                .clock(() -> Date.from(NOW))   // 고정 시각 기준으로 exp 검증 (실행 시각과 무관)
                .build()
                .parseSignedClaims(token);
    }

    @Test
    @DisplayName("[T1-12] 발급한 토큰을 같은 키로 파싱하면 iss=queue-drop, sub=userId, role, exp=iat+3600 이다")
    void issuesVerifiableToken() {
        // Given
        JwtTokenIssuer tokenIssuer = issuer(SECRET);

        // When
        IssuedToken token = tokenIssuer.issue(new TokenSubject(42L, "ADMIN"));

        // Then
        Jws<Claims> jws = parse(token.value());
        Claims claims = jws.getPayload();
        assertThat(jws.getHeader().getAlgorithm()).isEqualTo("HS256");
        assertThat(claims.getIssuer()).isEqualTo(ISSUER);
        assertThat(claims.getSubject()).isEqualTo("42");
        assertThat(claims.get("role", String.class)).isEqualTo("ADMIN");
        assertThat(claims.getIssuedAt().toInstant()).isEqualTo(NOW);
        assertThat(claims.getExpiration().toInstant()).isEqualTo(NOW.plusSeconds(3600));
        assertThat(token.expiresInSeconds()).isEqualTo(3600);
    }

    @Test
    @DisplayName("[T1-13] secret 이 31바이트면 생성 시 WeakKeyException 으로 실패한다 (기동 실패)")
    void rejectsShortSecret() {
        // Given
        String shortSecret = "a".repeat(31);

        // When / Then
        assertThatThrownBy(() -> issuer(shortSecret))
                .isInstanceOf(WeakKeyException.class);
    }

    @Test
    @DisplayName("[T1-13] secret 이 정확히 32바이트면 생성된다 (경계값)")
    void accepts32ByteSecret() {
        // Given
        String secret = "a".repeat(32);

        // When
        JwtTokenIssuer tokenIssuer = issuer(secret);

        // Then
        assertThat(tokenIssuer.issue(new TokenSubject(1L, "USER")).value()).isNotBlank();
    }
}
