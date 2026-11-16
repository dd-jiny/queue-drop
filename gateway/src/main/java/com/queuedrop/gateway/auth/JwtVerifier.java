package com.queuedrop.gateway.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;

/** 서명·만료·발급자 검증 */
@Component
public class JwtVerifier {

    private final JwtParser parser;

    public JwtVerifier(AuthProperties props) {
        SecretKey key = Keys.hmacShaKeyFor(props.jwt().secret().getBytes(StandardCharsets.UTF_8));
        this.parser = Jwts.parser().verifyWith(key).requireIssuer(props.jwt().issuer()).build();
    }

    /** @throws JwtVerificationException 만료(EXPIRED_TOKEN) 또는 그 외 실패(INVALID_TOKEN) */
    public VerifiedUser verify(String token) {
        try {
            Claims claims = parser.parseSignedClaims(token).getPayload();
            String role = claims.get("role", String.class);
            if (role == null) {
                throw new JwtException("missing role claim");
            }
            return new VerifiedUser(Long.parseLong(claims.getSubject()), role);
        } catch (ExpiredJwtException e) {
            throw new JwtVerificationException(GatewayErrorCode.EXPIRED_TOKEN, e);
        } catch (JwtException | IllegalArgumentException e) {   // 서명 위조, 형식 오류, sub 숫자 아님(NumberFormatException)
            throw new JwtVerificationException(GatewayErrorCode.INVALID_TOKEN, e);
        }
    }
}
