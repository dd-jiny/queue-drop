package com.queuedrop.user.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.user.application.dto.CreateAdminCommand;
import com.queuedrop.user.application.dto.LoginCommand;
import com.queuedrop.user.application.dto.LoginResult;
import com.queuedrop.user.application.dto.SignupCommand;
import com.queuedrop.user.application.dto.SignupResult;
import com.queuedrop.user.application.exception.UserErrorCode;
import com.queuedrop.user.application.port.IssuedToken;
import com.queuedrop.user.application.port.PasswordEncoder;
import com.queuedrop.user.application.port.TokenIssuer;
import com.queuedrop.user.application.port.TokenSubject;
import com.queuedrop.user.application.port.UserEventPublisher;
import com.queuedrop.user.application.port.UserRegisteredPayload;
import com.queuedrop.user.domain.exception.DuplicateEmailException;
import com.queuedrop.user.domain.exception.InvalidEmailException;
import com.queuedrop.user.domain.exception.InvalidNicknameException;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import com.queuedrop.user.domain.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/** 가입·로그인 유스케이스 흐름과 트랜잭션 경계 */
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final TokenIssuer tokenIssuer;
    private final UserEventPublisher eventPublisher;
    private final Clock clock;

    @Transactional
    public SignupResult signup(SignupCommand command) {
        Email email = toEmail(command.email(), UserErrorCode.INVALID_EMAIL);
        if (userRepository.existsByEmail(email)) {
            throw new ApplicationException(UserErrorCode.DUPLICATE_EMAIL);
        }

        User user;
        try {
            user = User.register(email, passwordEncoder.encode(command.rawPassword()),
                    command.nickname(), clock.instant());
        } catch (InvalidNicknameException e) {
            throw new ApplicationException(UserErrorCode.INVALID_NICKNAME, e);
        }

        User saved;
        try {
            saved = userRepository.save(user);
        } catch (DuplicateEmailException e) {          // exists 확인과 저장 사이 동시 가입
            throw new ApplicationException(UserErrorCode.DUPLICATE_EMAIL, e);
        }

        eventPublisher.publishRegistered(new UserRegisteredPayload(saved.getId(), saved.getNickname()));
        return SignupResult.from(saved);
    }

    /** 이메일 없음·비밀번호 불일치·형식 오류를 같은 코드로 응답한다 (계정 존재 여부 노출 방지) */
    @Transactional(readOnly = true)
    public LoginResult login(LoginCommand command) {
        Email email = toEmail(command.email(), UserErrorCode.INVALID_CREDENTIALS);
        User user = userRepository.findByEmail(email)
                .filter(u -> passwordEncoder.matches(command.rawPassword(), u.getPasswordHash()))
                .orElseThrow(() -> new ApplicationException(UserErrorCode.INVALID_CREDENTIALS));

        IssuedToken token = tokenIssuer.issue(new TokenSubject(user.getId(), user.getRole().name()));
        return new LoginResult(token.value(), "Bearer", token.expiresInSeconds());
    }

    /**
     * 관리자 계정이 없으면 생성한다. 기동 시 초기화 러너에서 호출.
     * @return 새로 만들었으면 true, 같은 이메일 계정이 이미 있으면 false (기존 계정의 역할은 바꾸지 않는다)
     */
    @Transactional
    public boolean createAdminIfAbsent(CreateAdminCommand command) {
        Email email = toEmail(command.email(), UserErrorCode.INVALID_EMAIL);
        if (userRepository.existsByEmail(email)) {
            return false;
        }
        User admin;
        try {
            admin = User.registerAdmin(email, passwordEncoder.encode(command.rawPassword()),
                    command.nickname(), clock.instant());
        } catch (InvalidNicknameException e) {
            throw new ApplicationException(UserErrorCode.INVALID_NICKNAME, e);
        }
        try {
            userRepository.save(admin);
        } catch (DuplicateEmailException e) {          // 인스턴스 여러 대가 동시에 기동한 경우
            return false;
        }
        return true;                                    // 관리자는 랭킹 대상이 아니므로 user.registered 를 발행하지 않는다
    }

    private Email toEmail(String raw, UserErrorCode onInvalid) {
        try {
            return new Email(raw);
        } catch (InvalidEmailException e) {
            throw new ApplicationException(onInvalid, e);
        }
    }
}
