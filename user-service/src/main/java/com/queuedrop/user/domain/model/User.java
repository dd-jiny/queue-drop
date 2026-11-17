package com.queuedrop.user.domain.model;

import com.queuedrop.user.domain.exception.InvalidNicknameException;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;

/** 회원 정보와 생성 규칙(닉네임 길이)을 책임진다. 원문 비밀번호는 domain 에 들어오지 않는다. */
@Getter
public class User {

    private static final int NICKNAME_MIN = 2;
    private static final int NICKNAME_MAX = 20;

    private final Long id;              // 저장 전 null
    private final Email email;
    private final String passwordHash;
    private final String nickname;
    private final Role role;
    private final Instant createdAt;

    private User(Long id, Email email, String passwordHash, String nickname, Role role, Instant createdAt) {
        this.id = id;
        this.email = Objects.requireNonNull(email);
        this.passwordHash = Objects.requireNonNull(passwordHash);
        this.nickname = validateNickname(nickname);
        this.role = Objects.requireNonNull(role);
        this.createdAt = Objects.requireNonNull(createdAt);
    }

    /** 신규 가입. 일반 가입은 항상 USER */
    public static User register(Email email, String passwordHash, String nickname, Instant now) {
        return new User(null, email, passwordHash, nickname, Role.USER, now);
    }

    /** 관리자 계정 생성. 기동 시 초기화에서만 사용 (일반 가입 경로로는 ADMIN 을 만들 수 없다) */
    public static User registerAdmin(Email email, String passwordHash, String nickname, Instant now) {
        return new User(null, email, passwordHash, nickname, Role.ADMIN, now);
    }

    /** 영속 데이터로부터 복원 (Mapper 전용) */
    public static User reconstitute(Long id, Email email, String passwordHash,
                                    String nickname, Role role, Instant createdAt) {
        return new User(id, email, passwordHash, nickname, role, createdAt);
    }

    private static String validateNickname(String nickname) {
        if (nickname == null) throw new InvalidNicknameException(null);
        String trimmed = nickname.trim();
        if (trimmed.length() < NICKNAME_MIN || trimmed.length() > NICKNAME_MAX) {
            throw new InvalidNicknameException(trimmed);
        }
        return trimmed;
    }
}
