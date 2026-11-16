package com.queuedrop.user.infrastructure.security;

import com.queuedrop.user.application.port.IssuedToken;
import com.queuedrop.user.application.port.TokenIssuer;
import com.queuedrop.user.application.port.TokenSubject;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;

@Component
@EnableConfigurationProperties(JwtProperties.class)
class JwtTokenIssuer implements TokenIssuer {

    private final JwtProperties props;
    private final Clock clock;
    private final SecretKey key;

    JwtTokenIssuer(JwtProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        this.key = Keys.hmacShaKeyFor(props.secret().getBytes(StandardCharsets.UTF_8));  // 32바이트 미만이면 기동 실패
    }

    @Override
    public IssuedToken issue(TokenSubject subject) {
        Instant now = clock.instant();
        Instant exp = now.plus(props.accessTokenTtl());
        String token = Jwts.builder()
                .issuer(props.issuer())
                .subject(String.valueOf(subject.userId()))
                .claim("role", subject.role())
                .issuedAt(Date.from(now))
                .expiration(Date.from(exp))
                .signWith(key)            // 키 길이에 맞춰 HS256
                .compact();
        return new IssuedToken(token, props.accessTokenTtl().toSeconds());
    }
}
