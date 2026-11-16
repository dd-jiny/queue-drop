package com.queuedrop.gateway.support;

import com.queuedrop.gateway.auth.AuthProperties;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * gateway 테스트용 JWT 생성기. user-service 코드에 의존하지 않고 jjwt 를 직접 사용한다.
 * JwtVerifier 는 시스템 시각으로 만료를 판단하므로, 토큰 시각은 실행 시점 기준 상대값(±1h)으로 넉넉히 잡는다.
 */
public final class TestTokens {

    public static final String SECRET = "gateway-test-secret-key-0123456789-abcdefghij";
    public static final String OTHER_SECRET = "another-test-secret-key-9876543210-zyxwvutsrq";
    public static final String ISSUER = "queue-drop";

    private static final Duration TTL = Duration.ofHours(1);

    private TestTokens() {}

    /** 이 토큰들을 검증하는 설정. 공개 경로는 application.yml 과 같은 목록 */
    public static AuthProperties authProperties() {
        return new AuthProperties(
                new AuthProperties.Jwt(SECRET, ISSUER),
                new AuthProperties.Auth(List.of(
                        "POST /api/users/signup",
                        "POST /api/users/login",
                        "GET /api/products/drops",
                        "GET /api/products/drops/{dropId}",
                        "GET /api/rankings/drops",
                        "GET /api/rankings/users/weekly",
                        "GET /api/rankings/users/all",
                        "GET /api/coupons/events/{eventId}",
                        "GET /api/entries/drops/{dropId}/result",
                        "GET /actuator/health")));
    }

    public static String valid(long userId, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(String.valueOf(userId))
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .signWith(key(SECRET))
                .compact();
    }

    public static String expired(long userId, String role) {
        Instant issuedAt = Instant.now().minus(TTL).minus(TTL);
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(String.valueOf(userId))
                .claim("role", role)
                .issuedAt(Date.from(issuedAt))
                .expiration(Date.from(issuedAt.plus(TTL)))   // 1시간 전에 만료
                .signWith(key(SECRET))
                .compact();
    }

    public static String signedWithOtherKey(long userId, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(String.valueOf(userId))
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .signWith(key(OTHER_SECRET))
                .compact();
    }

    public static String otherIssuer(long userId, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer("someone-else")
                .subject(String.valueOf(userId))
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .signWith(key(SECRET))
                .compact();
    }

    public static String withoutRole(long userId) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(String.valueOf(userId))
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .signWith(key(SECRET))
                .compact();
    }

    /** alg=none: 서명 없는 토큰 (header.payload.) */
    public static String unsigned(long userId, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(String.valueOf(userId))
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .compact();
    }

    /** 올바르게 서명됐지만 sub 가 임의 문자열인 토큰 */
    public static String withSubject(String subject, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .issuer(ISSUER)
                .subject(subject)
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(TTL)))
                .signWith(key(SECRET))
                .compact();
    }

    public static String malformed() {
        return "this.is-not.a-jwt";
    }

    private static SecretKey key(String secret) {
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }
}
