package com.queuedrop.user.application.service.fake;

import com.queuedrop.user.application.port.IssuedToken;
import com.queuedrop.user.application.port.TokenIssuer;
import com.queuedrop.user.application.port.TokenSubject;
import java.util.ArrayList;
import java.util.List;

/** 받은 subject 를 기록하고 고정 형식의 토큰을 돌려준다. */
public class FakeTokenIssuer implements TokenIssuer {

    public static final long EXPIRES_IN_SECONDS = 3600;

    private final List<TokenSubject> issuedSubjects = new ArrayList<>();

    @Override
    public IssuedToken issue(TokenSubject subject) {
        issuedSubjects.add(subject);
        return new IssuedToken("token-for-" + subject.userId(), EXPIRES_IN_SECONDS);
    }

    public void clear() {
        issuedSubjects.clear();
    }

    public List<TokenSubject> issuedSubjects() {
        return List.copyOf(issuedSubjects);
    }
}
