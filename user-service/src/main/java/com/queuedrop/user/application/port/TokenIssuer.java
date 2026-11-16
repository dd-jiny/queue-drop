package com.queuedrop.user.application.port;

public interface TokenIssuer {
    IssuedToken issue(TokenSubject subject);
}
