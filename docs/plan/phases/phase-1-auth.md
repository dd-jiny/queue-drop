# 1단계 — 인증 구현 상세 (user-service, gateway)

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | 관리자 계정 생성(ADMIN_EMAIL / ADMIN_PASSWORD 기동 시 생성) 확정 및 구현 상세·테스트·커밋 추가, 설계 반영 완료 표시 |
| v1.2 | 2026-11-16 | 0단계 결과 반영: 헤더 누락 400 `COMMON_MISSING_USER_HEADER`(3.8), Lombok/R1 설명 정정, user-service Kafka 블록을 0단계 공통 설정으로, jjwt 0.13.0. Access token 만료 1시간 기본안 확정. gateway 에러 코드 접두어 `AUTH_` 는 인증 도메인 이름으로 명명 규칙 예외 |
| v1.3 | 2026-11-16 | 단계 검증 반영: `createAdminIfAbsent` 트랜잭션 제거(동시 기동 시 UnexpectedRollbackException), 비밀번호 UTF-8 72바이트 상한(BCrypt), 3.7 gateway 구현 결정 기록, 리스크(JWT_SECRET fallback) 추가, 로그인 시 더미 해시 비교(E-15) |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 회원가입·로그인이 되고, 인증된 요청이 각 서비스에 `X-User-Id`, `X-User-Role` 헤더로 도착한다 |
| 요구사항 | REQ-USER-01 (회원가입), REQ-USER-02 (로그인·JWT 발급), REQ-USER-03 (Gateway JWT 검증·헤더 전달) |
| 브랜치 | `feat/phase-1-auth` (`dev` 에서 생성) |
| 선행 조건 | 0단계 완료 (common 에러 계약·이벤트 메시지, LayerRules, Testcontainers 설정, Flyway) |
| 참고 설계 | [architecture.md](../../design/02-architecture/architecture.md) 6장, [layered-architecture.md](../../design/02-architecture/layered-architecture.md), [sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 1장, [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.1·2.6, [erd.md](../../design/06-erd/erd.md) user_db |

**API**
| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| POST | `/api/users/signup` | 제외 | 회원가입 → 201 |
| POST | `/api/users/login` | 제외 | 로그인 → 200 `{accessToken, tokenType, expiresIn}` |

---

## 2. 산출물 파일 목록

```
user-service/
├── build.gradle                                        # 수정: Kafka, security-crypto, jjwt
└── src/
    ├── main/java/com/queuedrop/user/
    │   ├── domain/
    │   │   ├── model/
    │   │   │   ├── User.java
    │   │   │   ├── Email.java
    │   │   │   └── Role.java
    │   │   ├── exception/
    │   │   │   ├── InvalidEmailException.java
    │   │   │   ├── InvalidNicknameException.java
    │   │   │   └── DuplicateEmailException.java
    │   │   └── repository/
    │   │       └── UserRepository.java
    │   ├── application/
    │   │   ├── service/UserService.java
    │   │   ├── dto/
    │   │   │   ├── CreateAdminCommand.java
    │   │   │   ├── SignupCommand.java
    │   │   │   ├── SignupResult.java
    │   │   │   ├── LoginCommand.java
    │   │   │   └── LoginResult.java
    │   │   ├── port/
    │   │   │   ├── PasswordEncoder.java
    │   │   │   ├── TokenIssuer.java
    │   │   │   ├── TokenSubject.java
    │   │   │   ├── IssuedToken.java
    │   │   │   ├── UserEventPublisher.java
    │   │   │   └── UserRegisteredPayload.java
    │   │   └── exception/UserErrorCode.java
    │   ├── infrastructure/
    │   │   ├── persistence/
    │   │   │   ├── UserJpaEntity.java
    │   │   │   ├── UserJpaRepository.java
    │   │   │   ├── UserMapper.java
    │   │   │   └── UserRepositoryImpl.java
    │   │   ├── security/
    │   │   │   ├── BCryptPasswordEncoderAdapter.java
    │   │   │   ├── JwtProperties.java
    │   │   │   └── JwtTokenIssuer.java
    │   │   ├── messaging/KafkaUserEventPublisher.java
    │   │   └── config/ClockConfig.java
    │   └── presentation/
    │       ├── bootstrap/
    │       │   ├── AdminAccountProperties.java
    │       │   └── AdminAccountInitializer.java
    │       └── api/
    │           ├── UserController.java
    │           ├── GlobalExceptionHandler.java          # 0단계 산출물
    │           ├── request/
    │           │   ├── SignupRequest.java
    │           │   └── LoginRequest.java
    │           └── response/
    │               ├── SignupResponse.java
    │               └── LoginResponse.java
    ├── main/resources/
    │   ├── application.yml                              # 수정: Kafka 블록, JWT 설정
    │   └── db/migration/V1__create_users.sql
    └── test/java/com/queuedrop/user/
        ├── domain/model/EmailTest.java
        ├── domain/model/UserTest.java
        ├── application/service/UserServiceTest.java
        ├── application/service/fake/
        │   ├── InMemoryUserRepository.java
        │   ├── FakePasswordEncoder.java
        │   ├── FakeTokenIssuer.java
        │   └── RecordingUserEventPublisher.java
        ├── infrastructure/persistence/UserRepositoryImplTest.java
        ├── infrastructure/security/JwtTokenIssuerTest.java
        ├── presentation/api/UserControllerTest.java
        ├── presentation/bootstrap/AdminAccountInitializerTest.java
        ├── AdminBootstrapIntegrationTest.java
        └── SignupIntegrationTest.java

gateway/
├── build.gradle                                        # 수정: common, jjwt
└── src/
    ├── main/java/com/queuedrop/gateway/
    │   ├── filter/
    │   │   ├── HeaderSanitizeFilter.java
    │   │   ├── JwtAuthenticationFilter.java
    │   │   └── UserHeaders.java
    │   ├── auth/
    │   │   ├── JwtVerifier.java
    │   │   ├── VerifiedUser.java
    │   │   ├── JwtVerificationException.java
    │   │   ├── PublicPathMatcher.java
    │   │   └── GatewayErrorCode.java
    │   └── config/
    │       ├── AuthProperties.java
    │       └── ErrorResponseWriter.java
    ├── main/resources/application.yml                  # 수정: JWT, 인증 제외 경로
    └── test/java/com/queuedrop/gateway/
        ├── filter/HeaderSanitizeFilterTest.java
        ├── filter/JwtAuthenticationFilterTest.java
        ├── auth/PublicPathMatcherTest.java
        └── support/TestTokens.java

.env.example                                            # 신규: JWT_SECRET 예시
```

---

## 3. 구현 상세

### 3.1 빌드

#### `user-service/build.gradle` (전문)
```groovy
dependencies {
    implementation project(':common')
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    implementation 'org.springframework.boot:spring-boot-starter-flyway'
    implementation 'org.flywaydb:flyway-database-postgresql'
    implementation 'org.springframework.boot:spring-boot-starter-kafka'          // user.registered 발행
    implementation 'org.springframework.security:spring-security-crypto'        // BCrypt 만 사용 (Security 필터 체인 X)

    implementation 'io.jsonwebtoken:jjwt-api:0.13.0'
    runtimeOnly 'io.jsonwebtoken:jjwt-impl:0.13.0'
    runtimeOnly 'io.jsonwebtoken:jjwt-gson:0.13.0'

    runtimeOnly 'org.postgresql:postgresql'

    testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'
    testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
}
```

#### `gateway/build.gradle` (전문)
```groovy
dependencies {
    implementation project(':common')                                            // ErrorCode, ErrorResponse
    implementation 'org.springframework.cloud:spring-cloud-starter-gateway-server-webflux'

    implementation 'io.jsonwebtoken:jjwt-api:0.13.0'
    runtimeOnly 'io.jsonwebtoken:jjwt-impl:0.13.0'
    runtimeOnly 'io.jsonwebtoken:jjwt-gson:0.13.0'

    testImplementation 'io.projectreactor:reactor-test'
}
```

(구현 결정) jjwt 직렬화 모듈로 `jjwt-jackson` 대신 **`jjwt-gson`** 을 쓴다. Boot 4 는 Jackson 3(`tools.jackson`)을 쓰는데 `jjwt-jackson` 은 Jackson 2 를 끌어와 두 버전이 섞인다. Gson 버전은 Boot BOM 이 관리한다.

### 3.2 JWT 설계

| 항목 | 값 |
|------|-----|
| 알고리즘 | HS256 (대칭키) |
| 비밀키 | 환경변수 `JWT_SECRET` (UTF-8 바이트, **32바이트 이상**) — user-service(서명)와 gateway(검증)가 같은 값 사용 |
| `iss` | `queue-drop` |
| `sub` | userId (문자열) |
| `role` | `USER` \| `ADMIN` |
| `iat` / `exp` | 발급 시각 / 발급 + **1시간** (구현 결정) |
| Refresh token | 없음 (범위 밖, 만료 시 재로그인) |

토큰 페이로드 예:
```json
{ "iss": "queue-drop", "sub": "42", "role": "USER", "iat": 1794826800, "exp": 1794830400 }
```

**JWT_SECRET 공유 방식**
- 로컬(IDE 실행): 두 서비스 `application.yml` 에 같은 개발용 기본값을 둔다 → 설정 없이 바로 동작.
- 컨테이너 실행(8단계): 루트 `.env`(git 제외, `.gitignore` 에 이미 포함)에 `JWT_SECRET` 을 두고 compose 가 두 서비스에 주입.
- 저장소에는 `.env.example` 만 커밋한다.

```bash
# .env.example
# 32바이트 이상의 임의 문자열. 운영에서는 반드시 교체한다.
JWT_SECRET=change-me-to-a-random-string-at-least-32-bytes
```

### 3.3 user-service — domain

#### `Role`
```java
package com.queuedrop.user.domain.model;

public enum Role { USER, ADMIN }
```

#### `Email` — 형식 검증·정규화를 책임지는 값 객체
```java
package com.queuedrop.user.domain.model;

import com.queuedrop.user.domain.exception.InvalidEmailException;
import java.util.Locale;
import java.util.regex.Pattern;

public record Email(String value) {

    private static final Pattern FORMAT = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    private static final int MAX_LENGTH = 255;

    public Email {
        if (value == null) throw new InvalidEmailException(null);
        value = value.trim().toLowerCase(Locale.ROOT);   // 대소문자 무관하게 같은 이메일
        if (value.length() > MAX_LENGTH || !FORMAT.matcher(value).matches()) {
            throw new InvalidEmailException(value);
        }
    }
}
```

#### `User` — 회원 정보와 생성 규칙(닉네임 길이)을 책임진다
```java
package com.queuedrop.user.domain.model;

import com.queuedrop.user.domain.exception.InvalidNicknameException;
import lombok.Getter;
import java.time.Instant;
import java.util.Objects;

@Getter
public class User {

    private static final int NICKNAME_MIN = 2;
    private static final int NICKNAME_MAX = 20;

    private final Long id;              // 저장 전 null
    private final Email email;
    private final String passwordHash;  // 원문 비밀번호는 domain 에 들어오지 않는다
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
```
- `@Getter` 는 Lombok compileOnly 다. 생성 메서드에 `@lombok.Generated` 가 남지만 R1 이 `lombok..` 을 허용한다 (layered-architecture.md v1.2).
- 닉네임 중복은 허용한다 (요구사항에 없음).

#### 도메인 예외 (패키지 `domain.exception`, 순수 Java)
```java
package com.queuedrop.user.domain.exception;

public class InvalidEmailException extends RuntimeException {
    public InvalidEmailException(String value) { super("invalid email: " + value); }
}
```
```java
public class InvalidNicknameException extends RuntimeException {
    public InvalidNicknameException(String value) { super("invalid nickname: " + value); }
}
```
```java
/** 저장 시 이메일 유니크 위반. infrastructure 가 DB 예외를 이 예외로 번역한다. */
public class DuplicateEmailException extends RuntimeException {
    public DuplicateEmailException(String email) { super("duplicate email: " + email); }
}
```

#### `UserRepository` — 영속성 포트 (domain)
```java
package com.queuedrop.user.domain.repository;

import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import java.util.Optional;

public interface UserRepository {
    /** @throws com.queuedrop.user.domain.exception.DuplicateEmailException 이메일 유니크 위반 */
    User save(User user);
    Optional<User> findByEmail(Email email);
    boolean existsByEmail(Email email);
}
```

### 3.4 user-service — application

#### DTO (Command / Result) — public 시그니처는 이 타입만 쓴다 (R6)
```java
package com.queuedrop.user.application.dto;

public record SignupCommand(String email, String rawPassword, String nickname) {}
```
```java
public record LoginCommand(String email, String rawPassword) {}
```
```java
public record CreateAdminCommand(String email, String rawPassword, String nickname) {}
```
```java
package com.queuedrop.user.application.dto;

import com.queuedrop.user.domain.model.User;

public record SignupResult(Long userId, String email, String nickname) {
    public static SignupResult from(User user) {
        return new SignupResult(user.getId(), user.getEmail().value(), user.getNickname());
    }
}
```
```java
public record LoginResult(String accessToken, String tokenType, long expiresInSeconds) {}
```

#### 포트 (외부 연동, application/port)
```java
package com.queuedrop.user.application.port;

/** Spring Security 의 PasswordEncoder 를 직접 쓰지 않기 위한 포트 */
public interface PasswordEncoder {
    String encode(String rawPassword);
    boolean matches(String rawPassword, String passwordHash);
}
```
```java
public interface TokenIssuer {
    IssuedToken issue(TokenSubject subject);
}

public record TokenSubject(Long userId, String role) {}
public record IssuedToken(String value, long expiresInSeconds) {}
```
```java
public interface UserEventPublisher {
    /** 트랜잭션 커밋 후 발행된다 (구현체 책임). 커밋 전 롤백되면 발행하지 않는다. */
    void publishRegistered(UserRegisteredPayload payload);
}

public record UserRegisteredPayload(Long userId, String nickname) {}
```
> application 은 `common.event.UserRegistered` 를 모른다. 자체 Payload 를 넘기고 infrastructure 가 메시지로 변환한다 ([layered-architecture.md](../../design/02-architecture/layered-architecture.md) 4.4).

#### 에러 코드
```java
package com.queuedrop.user.application.exception;

import com.queuedrop.common.error.ErrorCode;

public enum UserErrorCode implements ErrorCode {
    DUPLICATE_EMAIL("USER_DUPLICATE_EMAIL", 409, "이미 가입된 이메일입니다."),
    INVALID_EMAIL("USER_INVALID_EMAIL", 400, "이메일 형식이 올바르지 않습니다."),
    INVALID_NICKNAME("USER_INVALID_NICKNAME", 400, "닉네임은 2~20자여야 합니다."),
    INVALID_CREDENTIALS("USER_INVALID_CREDENTIALS", 401, "이메일 또는 비밀번호가 올바르지 않습니다.");

    // 필드·생성자·getter 는 CommonErrorCode 와 동일 패턴
    // ...
}
```
- 로그인 실패는 "이메일 없음"과 "비밀번호 불일치"를 **같은 코드**로 응답한다 (계정 존재 여부 노출 방지).

#### `UserService` — 가입·로그인 유스케이스 흐름과 트랜잭션 경계
```java
package com.queuedrop.user.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.user.application.dto.*;
import com.queuedrop.user.application.exception.UserErrorCode;
import com.queuedrop.user.application.port.*;
import com.queuedrop.user.domain.exception.*;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import com.queuedrop.user.domain.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Clock;

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
     *
     * 트랜잭션을 걸지 않는다: 동시 기동 경쟁에서 저장이 유니크 위반으로 실패하면 저장소 호출의 트랜잭션 안에서 롤백이 끝나고
     * false 를 반환한다. 바깥 @Transactional 이 있으면 rollback-only 표시 때문에 커밋 시 UnexpectedRollbackException 으로 기동이 실패한다.
     */
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
        return true;                                    // 관리자는 랭킹 대상이 아니므로 user.registered 를 발행하지 않는다 (구현 결정)
    }

    private Email toEmail(String raw, UserErrorCode onInvalid) {
        try {
            return new Email(raw);
        } catch (InvalidEmailException e) {
            throw new ApplicationException(onInvalid, e);
        }
    }
}
```
- `Clock` 은 `java.time` 이라 R3 에 걸리지 않는다. 테스트에서 고정 시각 주입.

### 3.5 user-service — infrastructure

#### Flyway `V1__create_users.sql` (전문)
```sql
CREATE TABLE users (
    id            BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    email         VARCHAR(255) NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    nickname      VARCHAR(20)  NOT NULL,
    role          VARCHAR(10)  NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_users_email UNIQUE (email),
    CONSTRAINT ck_users_role  CHECK (role IN ('USER', 'ADMIN'))
);
```
- `email` 은 도메인에서 소문자 정규화 후 저장하므로 대소문자 구분 유니크로 충분하다.
- `Instant` ↔ `TIMESTAMP WITH TIME ZONE`. `ddl-auto: validate` 가 타입 불일치를 잡는지 T1-09 에서 확인.

#### `UserJpaEntity` — users 테이블 매핑 (R7: persistence 패키지)
```java
package com.queuedrop.user.infrastructure.persistence;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Entity
@Table(name = "users")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UserJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 255, unique = true)
    private String email;

    @Column(name = "password_hash", nullable = false, length = 100)
    private String passwordHash;

    @Column(nullable = false, length = 20)
    private String nickname;

    @Column(nullable = false, length = 10)
    private String role;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    UserJpaEntity(Long id, String email, String passwordHash, String nickname, String role, Instant createdAt) {
        // ... 필드 대입
    }
}
```

#### `UserJpaRepository` — Spring Data
```java
interface UserJpaRepository extends JpaRepository<UserJpaEntity, Long> {
    Optional<UserJpaEntity> findByEmail(String email);
    boolean existsByEmail(String email);
}
```

#### `UserMapper` — 도메인 ↔ 엔티티 변환
```java
@Component
class UserMapper {
    User toDomain(UserJpaEntity e) {
        return User.reconstitute(e.getId(), new Email(e.getEmail()), e.getPasswordHash(),
                                 e.getNickname(), Role.valueOf(e.getRole()), e.getCreatedAt());
    }

    UserJpaEntity toEntity(User u) {
        return new UserJpaEntity(u.getId(), u.getEmail().value(), u.getPasswordHash(),
                                 u.getNickname(), u.getRole().name(), u.getCreatedAt());
    }
}
```

#### `UserRepositoryImpl` — domain 포트 구현, DB 예외 번역
```java
@Repository
@RequiredArgsConstructor
class UserRepositoryImpl implements UserRepository {

    private final UserJpaRepository jpa;
    private final UserMapper mapper;

    @Override
    public User save(User user) {
        try {
            // saveAndFlush: 유니크 위반을 이 메서드 안에서 터뜨려 번역하기 위해 즉시 flush
            return mapper.toDomain(jpa.saveAndFlush(mapper.toEntity(user)));
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateEmailException(user.getEmail().value());
        }
    }

    @Override
    public Optional<User> findByEmail(Email email) {
        return jpa.findByEmail(email.value()).map(mapper::toDomain);
    }

    @Override
    public boolean existsByEmail(Email email) {
        return jpa.existsByEmail(email.value());
    }
}
```
> users 테이블의 무결성 제약은 email 유니크와 role CHECK 뿐이고 role 은 enum 이라 위반 불가 → `DataIntegrityViolationException` 을 이메일 중복으로 번역해도 안전하다 (구현 결정). 제약이 늘면 제약 이름(`uk_users_email`)으로 구분한다.

#### `BCryptPasswordEncoderAdapter`
```java
@Component
class BCryptPasswordEncoderAdapter implements PasswordEncoder {
    private final BCryptPasswordEncoder delegate = new BCryptPasswordEncoder();   // strength 10 (기본)

    @Override public String encode(String raw) { return delegate.encode(raw); }
    @Override public boolean matches(String raw, String hash) { return delegate.matches(raw, hash); }
}
```

#### `JwtProperties` / `JwtTokenIssuer`
```java
@ConfigurationProperties(prefix = "queuedrop.jwt")
public record JwtProperties(String secret, String issuer, Duration accessTokenTtl) {}
```
```java
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
```

#### `KafkaUserEventPublisher` — 커밋 후 발행
```java
@Slf4j
@Component
@RequiredArgsConstructor
class KafkaUserEventPublisher implements UserEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publishRegistered(UserRegisteredPayload payload) {
        UserRegistered message = new UserRegistered(payload.userId(), payload.nickname());
        Runnable send = () -> kafkaTemplate.send(Topics.USER_REGISTERED, String.valueOf(payload.userId()), message)
                .whenComplete((r, ex) -> {
                    if (ex != null) log.warn("user.registered publish failed: userId={}", payload.userId(), ex);
                });

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() { send.run(); }
            });
        } else {
            send.run();
        }
    }
}
```
(구현 결정) "커밋 후 발행"을 application 의 `@TransactionalEventListener` 가 아니라 **infrastructure 어댑터 안에서** `TransactionSynchronization` 으로 처리한다. application 은 "발행한다"만 알고 시점은 구현체 책임 → application 이 Spring 이벤트 API 에 의존하지 않는다. 2단계 `DropEventPublisher` 도 같은 패턴을 쓴다.
- 회원가입 이벤트는 **비동기 발행(응답 대기 X)**. 실패 시 랭킹 닉네임만 누락되므로 로그로 남긴다 (응모의 동기 발행과 다름).

#### `ClockConfig`
```java
@Configuration
class ClockConfig {
    @Bean Clock clock() { return Clock.systemUTC(); }
}
```

#### `application.yml` (전문)
```yaml
server:
  port: 8081

spring:
  application:
    name: user-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/user_db
    username: ${DB_USERNAME:queuedrop}
    password: ${DB_PASSWORD:queuedrop}
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:                          # 0단계 3.9 공통 producer 블록 (user-service 는 발행만 한다)
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
        request.timeout.ms: 1000
        delivery.timeout.ms: 3000
        max.block.ms: 1000

queuedrop:
  jwt:
    secret: ${JWT_SECRET:queue-drop-local-development-secret-key-0123456789}   # gateway 와 동일해야 함
    issuer: queue-drop
    access-token-ttl: 1h
  admin:
    email: ${ADMIN_EMAIL:}            # 비어 있으면 관리자 생성 건너뜀
    password: ${ADMIN_PASSWORD:}
    nickname: ${ADMIN_NICKNAME:admin}
```

### 3.6 user-service — presentation

#### Request / Response
```java
package com.queuedrop.user.presentation.api.request;

public record SignupRequest(
        @NotBlank @Email @Size(max = 255) String email,
        @NotBlank @Size(min = 8, max = 64) String password,
        @NotBlank @Size(min = 2, max = 20) String nickname
) {
    public SignupCommand toCommand() { return new SignupCommand(email, password, nickname); }
}
```
```java
public record LoginRequest(@NotBlank String email, @NotBlank String password) {
    public LoginCommand toCommand() { return new LoginCommand(email, password); }
}
```
```java
package com.queuedrop.user.presentation.api.response;

public record SignupResponse(Long userId, String email, String nickname) {
    public static SignupResponse from(SignupResult r) { return new SignupResponse(r.userId(), r.email(), r.nickname()); }
}

public record LoginResponse(String accessToken, String tokenType, long expiresIn) {
    public static LoginResponse from(LoginResult r) {
        return new LoginResponse(r.accessToken(), r.tokenType(), r.expiresInSeconds());
    }
}
```
- 비밀번호 정책(8~64자, **UTF-8 72바이트 이하** — BCrypt 는 72바이트 초과 `encode` 를 거부한다)은 **입력 형식**이라 presentation `@Valid`(`@MaxUtf8Bytes(72)` 커스텀 제약)에서 검증한다. 관리자 러너도 같은 기준. domain 은 원문 비밀번호를 받지 않으므로 검증할 수 없다 (구현 결정).
- 이메일·닉네임은 presentation(형식)과 domain(규칙) 양쪽에서 검증한다. domain 검증이 최종 규칙이다.

#### `UserController`
```java
@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/signup")
    public ResponseEntity<SignupResponse> signup(@Valid @RequestBody SignupRequest request) {
        SignupResult result = userService.signup(request.toCommand());
        return ResponseEntity.status(HttpStatus.CREATED).body(SignupResponse.from(result));
    }

    @PostMapping("/login")
    public LoginResponse login(@Valid @RequestBody LoginRequest request) {
        return LoginResponse.from(userService.login(request.toCommand()));
    }
}
```

#### 관리자 계정 초기화 (확정, 2026-11-16)
기동 시 `ADMIN_EMAIL` / `ADMIN_PASSWORD` 가 있으면 관리자 계정을 **없을 때만** 만든다. 드로우·쿠폰 이벤트 등록(ADMIN 권한)의 전제.

```java
package com.queuedrop.user.presentation.bootstrap;

@ConfigurationProperties(prefix = "queuedrop.admin")
public record AdminAccountProperties(String email, String password, String nickname) {
    public boolean enabled() { return email != null && !email.isBlank(); }
}
```
```java
package com.queuedrop.user.presentation.bootstrap;

import com.queuedrop.user.application.dto.CreateAdminCommand;
import com.queuedrop.user.application.service.UserService;

/** 기동이라는 '입력'을 받아 application 을 호출하는 입력 어댑터 → presentation (Scheduler·Listener 와 같은 위치) */
@Slf4j
@Component
@RequiredArgsConstructor
@EnableConfigurationProperties(AdminAccountProperties.class)
class AdminAccountInitializer implements ApplicationRunner {

    private static final int PASSWORD_MIN = 8;

    private final AdminAccountProperties properties;
    private final UserService userService;

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.enabled()) {
            log.info("ADMIN_EMAIL not set — skip admin bootstrap");
            return;
        }
        if (properties.password() == null || properties.password().length() < PASSWORD_MIN) {
            throw new IllegalStateException("ADMIN_PASSWORD must be at least " + PASSWORD_MIN + " characters");  // 기동 실패로 설정 오류를 빨리 드러냄
        }
        boolean created = userService.createAdminIfAbsent(
                new CreateAdminCommand(properties.email(), properties.password(), properties.nickname()));
        log.info(created ? "admin account created: {}" : "admin account already exists: {}", properties.email());
    }
}
```
- presentation 은 `CreateAdminCommand`(application DTO)만 알고 `User`/`Role`(domain)은 모른다 (R4).
- 같은 이메일의 일반 회원이 이미 있으면 **역할을 승격하지 않고** false 를 반환한다. 권한 상승 경로를 만들지 않기 위함 (구현 결정).
- 비밀번호 길이 검증은 회원가입과 같은 기준(8자 이상)을 러너에서 한다 — domain 은 원문 비밀번호를 받지 않는다.

### 3.7 gateway

gateway 는 계층 구조를 적용하지 않는다 ([class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.6). 패키지는 `filter`(GlobalFilter), `auth`(검증·경로 판단), `config`.

#### 필터 순서
| 순서 | 필터 | `getOrder()` | 동작 |
|------|------|--------------|------|
| 1 | `HeaderSanitizeFilter` | `Ordered.HIGHEST_PRECEDENCE` | 클라이언트가 보낸 `X-User-Id`, `X-User-Role` **항상 제거** |
| 2 | `JwtAuthenticationFilter` | `Ordered.HIGHEST_PRECEDENCE + 1` | 토큰 검증 → 헤더 주입 / 401 |

> 제거가 검증보다 **먼저**여야 한다. 검증 필터의 어떤 경로(인증 제외 경로 포함)에서도 위조 헤더가 남지 않는다 ([sequence-diagram.md](../../design/08-sequence-diagram/sequence-diagram.md) 1장).
> Spring Cloud Gateway 내장 `RemoveCachedBodyFilter` 도 `HIGHEST_PRECEDENCE` 라 정렬상 두 필터 사이에 올 수 있다. 캐시된 바디만 정리하고 헤더는 다루지 않으므로 영향 없음 (구현 확인).

**구현 결정 (문서 코드와 다른 점)**
- 헤더 주입은 `headers(h -> h.set(...))` — 추가가 아니라 교체. 제거 필터가 앞에 있지만 단독으로도 값이 하나만 남게 한다.
- `role` 클레임이 없는 토큰은 `INVALID_TOKEN`.
- `Bearer` 접두어는 대소문자 무시, `Bearer ` 뒤가 비어 있으면 토큰 없음(`TOKEN_MISSING`) 으로 본다.
- `@EnableConfigurationProperties(AuthProperties)` 는 `config/AuthConfig` 에 둔다.
- `JwtVerifier` 는 jjwt 기본 시스템 시각으로 만료를 판단한다. 테스트 토큰은 실행 시점 기준 ±1시간으로 만든다 (고정 Clock 규칙 예외 — 경계 시각 테스트가 필요해지면 `parser().clock(...)` 주입).

#### 인증 제외 경로
| Method | Path 패턴 | 근거 |
|--------|-----------|------|
| POST | `/api/users/signup` | REQ-USER-01 |
| POST | `/api/users/login` | REQ-USER-02 |
| GET | `/api/products/drops`, `/api/products/drops/{dropId}` | REQ-DROP-03 |
| GET | `/api/rankings/drops`, `/api/rankings/users/weekly`, `/api/rankings/users/all` | REQ-RANK-01, 02 (`/api/rankings/users/me` 는 인증 필요) |
| GET | `/api/coupons/events/{eventId}` | coupon-issue.md API 표 "공개" (구현 결정 — 설계 수정 필요) |
| GET | `/api/entries/drops/{dropId}/result` | 추첨 요약 공개 (구현 결정 — 설계 수정 필요) |
| GET | `/actuator/health` | 헬스체크 |

**인증 제외 경로의 토큰 처리** (구현 결정)
- 토큰 없음 → 헤더 없이 통과 (익명)
- 유효한 토큰 → 헤더 주입 후 통과
- 잘못된/만료된 토큰 → **익명으로 통과** (공개 API 는 토큰 상태와 무관하게 응답)

#### `application.yml` 추가분
```yaml
queuedrop:
  jwt:
    secret: ${JWT_SECRET:queue-drop-local-development-secret-key-0123456789}   # user-service 와 동일
    issuer: queue-drop
  auth:
    public-paths:
      - POST /api/users/signup
      - POST /api/users/login
      - GET /api/products/drops
      - GET /api/products/drops/{dropId}
      - GET /api/rankings/drops
      - GET /api/rankings/users/weekly
      - GET /api/rankings/users/all
      - GET /api/coupons/events/{eventId}
      - GET /api/entries/drops/{dropId}/result
      - GET /actuator/health
```
기존 `server.port`, `spring.cloud.gateway.server.webflux.routes` 는 유지.

#### `UserHeaders`
```java
public final class UserHeaders {
    public static final String USER_ID = "X-User-Id";
    public static final String USER_ROLE = "X-User-Role";
    private UserHeaders() {}
}
```

#### `HeaderSanitizeFilter`
```java
@Component
public class HeaderSanitizeFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest sanitized = exchange.getRequest().mutate()
                .headers(h -> {                       // HttpHeaders 는 대소문자 무시
                    h.remove(UserHeaders.USER_ID);
                    h.remove(UserHeaders.USER_ROLE);
                })
                .build();
        return chain.filter(exchange.mutate().request(sanitized).build());
    }

    @Override
    public int getOrder() { return Ordered.HIGHEST_PRECEDENCE; }
}
```

#### `GatewayErrorCode`
```java
public enum GatewayErrorCode implements ErrorCode {
    TOKEN_MISSING("AUTH_TOKEN_MISSING", 401, "인증 토큰이 필요합니다."),
    INVALID_TOKEN("AUTH_INVALID_TOKEN", 401, "유효하지 않은 인증 토큰입니다."),
    EXPIRED_TOKEN("AUTH_EXPIRED_TOKEN", 401, "인증 토큰이 만료되었습니다.");
    // ...
}
```

#### `JwtVerifier` — 서명·만료·발급자 검증
```java
@Component
@EnableConfigurationProperties(AuthProperties.class)
public class JwtVerifier {

    private final JwtParser parser;

    public JwtVerifier(AuthProperties props) {
        SecretKey key = Keys.hmacShaKeyFor(props.jwt().secret().getBytes(StandardCharsets.UTF_8));
        this.parser = Jwts.parser().verifyWith(key).requireIssuer(props.jwt().issuer()).build();
    }

    /** @throws JwtVerificationException 만료(EXPIRED) 또는 그 외 실패(INVALID) */
    public VerifiedUser verify(String token) {
        try {
            Claims claims = parser.parseSignedClaims(token).getPayload();
            return new VerifiedUser(Long.parseLong(claims.getSubject()), claims.get("role", String.class));
        } catch (ExpiredJwtException e) {
            throw new JwtVerificationException(GatewayErrorCode.EXPIRED_TOKEN, e);
        } catch (JwtException | IllegalArgumentException e) {   // 서명 위조, 형식 오류, sub 숫자 아님
            throw new JwtVerificationException(GatewayErrorCode.INVALID_TOKEN, e);
        }
    }
}

public record VerifiedUser(Long userId, String role) {}
```

#### `AuthProperties`
```java
@ConfigurationProperties(prefix = "queuedrop")
public record AuthProperties(Jwt jwt, Auth auth) {
    public record Jwt(String secret, String issuer) {}
    public record Auth(List<String> publicPaths) {}
}
```

#### `PublicPathMatcher` — "METHOD 패턴" 목록으로 인증 제외 여부 판단
```java
@Component
public class PublicPathMatcher {

    private record Rule(HttpMethod method, PathPattern pattern) {}
    private final List<Rule> rules;

    public PublicPathMatcher(AuthProperties props) {
        PathPatternParser parser = PathPatternParser.defaultInstance;
        this.rules = props.auth().publicPaths().stream()
                .map(s -> s.split(" ", 2))
                .map(p -> new Rule(HttpMethod.valueOf(p[0]), parser.parse(p[1])))
                .toList();
    }

    public boolean isPublic(HttpMethod method, PathContainer path) {
        return rules.stream().anyMatch(r -> r.method().equals(method) && r.pattern().matches(path));
    }
}
```

#### `JwtAuthenticationFilter`
```java
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

    private static final String BEARER = "Bearer ";

    private final JwtVerifier verifier;
    private final PublicPathMatcher publicPaths;
    private final ErrorResponseWriter errorWriter;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        boolean isPublic = publicPaths.isPublic(request.getMethod(), request.getPath().pathWithinApplication());
        String token = resolveToken(request);

        if (token == null) {
            return isPublic ? chain.filter(exchange)
                            : errorWriter.write(exchange, GatewayErrorCode.TOKEN_MISSING);
        }

        VerifiedUser user;
        try {
            user = verifier.verify(token);
        } catch (JwtVerificationException e) {
            return isPublic ? chain.filter(exchange)                       // 공개 경로는 익명 통과
                            : errorWriter.write(exchange, e.errorCode());
        }

        ServerHttpRequest authenticated = request.mutate()
                .header(UserHeaders.USER_ID, String.valueOf(user.userId()))
                .header(UserHeaders.USER_ROLE, user.role())
                .build();
        return chain.filter(exchange.mutate().request(authenticated).build());
    }

    private String resolveToken(ServerHttpRequest request) {
        String header = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        return (header != null && header.startsWith(BEARER)) ? header.substring(BEARER.length()) : null;
    }

    @Override
    public int getOrder() { return Ordered.HIGHEST_PRECEDENCE + 1; }
}
```

#### `ErrorResponseWriter` — 401 을 공통 `ErrorResponse` JSON 으로 응답
```java
@Component
@RequiredArgsConstructor
public class ErrorResponseWriter {

    private final JsonMapper jsonMapper;     // Boot 4 자동 구성 (Jackson 3, tools.jackson) — 구현 시 확인

    public Mono<Void> write(ServerWebExchange exchange, ErrorCode code) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatusCode.valueOf(code.status()));
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = jsonMapper.writeValueAsBytes(ErrorResponse.of(code));
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}
```

### 3.8 다른 서비스가 헤더를 받는 방식 (참고, 2단계 이후 적용)
```java
@PostMapping("/drops/{dropId}")
public ResponseEntity<EntryApplyResponse> apply(@PathVariable Long dropId,
                                                @RequestHeader("X-User-Id") Long userId) { /* ... */ }
```
- 헤더가 없으면 `MissingRequestHeaderException` → 0단계 `GlobalExceptionHandler` 가 400 `COMMON_MISSING_USER_HEADER` 응답 (layered-architecture.md 4.5).
- 관리자 API 는 `@RequestHeader("X-User-Role") String role` 을 받아 Command 에 담고, **application 이 권한을 검사**한다 (2단계 `DropService`).

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ |
|----|-----------|---------------|--------------------------------|----------|
| T1-01 | domain 단위 | `EmailTest` | Given `" User@Example.COM "` / When `new Email()` / Then 값 `user@example.com` | REQ-USER-01 |
| T1-02 | domain 단위 | `EmailTest` | Given `null`, `"a@b"`, `"no-at.com"`, 256자 이메일 / When 생성 / Then `InvalidEmailException` | REQ-USER-01 |
| T1-03 | domain 단위 | `UserTest` | Given 닉네임 1자, 21자, 공백만 / When `User.register` / Then `InvalidNicknameException`. 2자·20자는 성공, 앞뒤 공백 제거 | REQ-USER-01 |
| T1-04 | domain 단위 | `UserTest` | When `User.register` / Then role=USER, id=null | REQ-USER-01 |
| T1-05 | application 단위 | `UserServiceTest` (Fake 포트) | Given 빈 저장소 / When signup / Then 저장된 passwordHash ≠ 원문, `RecordingUserEventPublisher` 에 payload 1건, Result 에 userId | REQ-USER-01 |
| T1-06 | application 단위 | `UserServiceTest` | Given 같은 이메일(대소문자만 다름) 가입됨 / When signup / Then `USER_DUPLICATE_EMAIL`, 이벤트 0건 | REQ-USER-01 |
| T1-07 | application 단위 | `UserServiceTest` | Given `InMemoryUserRepository.save` 가 `DuplicateEmailException` 을 던지도록 설정 (동시 가입 재현) / When signup / Then `USER_DUPLICATE_EMAIL` | REQ-USER-01 |
| T1-08 | application 단위 | `UserServiceTest` | Given 가입된 사용자 / When 올바른 비밀번호로 login / Then `FakeTokenIssuer` 가 받은 subject = (userId, "USER"), tokenType=Bearer | REQ-USER-02 |
| T1-09 | application 단위 | `UserServiceTest` | When 비밀번호 불일치 / 없는 이메일 / 형식 오류 이메일로 login / Then 세 경우 모두 `USER_INVALID_CREDENTIALS` | REQ-USER-02 |
| T1-10 | infra 통합 | `UserRepositoryImplTest` (`@DataJpaTest` + `PostgresContainerConfig`) | Given Flyway V1 / When save → findByEmail / Then 필드 일치 (Instant 정밀도 포함), `ddl-auto: validate` 통과 | REQ-USER-01 |
| T1-11 | infra 통합 | `UserRepositoryImplTest` | Given 같은 이메일 저장됨 / When 다시 save / Then `DuplicateEmailException` | REQ-USER-01 |
| T1-12 | infra 단위 | `JwtTokenIssuerTest` | Given 고정 Clock, TTL 1h / When issue / Then 같은 키로 파싱 시 iss=queue-drop, sub=userId, role, exp=iat+3600 | REQ-USER-02 |
| T1-13 | infra 단위 | `JwtTokenIssuerTest` | Given 31바이트 secret / When 생성 / Then 예외 (기동 실패) | REQ-USER-02 |
| T1-14 | presentation 슬라이스 | `UserControllerTest` (`@WebMvcTest`, UserService mock) | When 비밀번호 7자 / 이메일 형식 오류로 signup / Then 400 `COMMON_INVALID_INPUT` + fieldErrors | REQ-USER-01 |
| T1-15 | presentation 슬라이스 | `UserControllerTest` | Given service 가 `ApplicationException(DUPLICATE_EMAIL)` / When signup / Then 409 `USER_DUPLICATE_EMAIL` | REQ-USER-01 |
| T1-16 | 통합 | `SignupIntegrationTest` (`@SpringBootTest` + PG + Kafka) | When POST `/api/users/signup` / Then 201, users 1행, `user.registered` 토픽에 `{userId, nickname}` 1건 (key=userId) | REQ-USER-01 |
| T1-17 | 통합 | `SignupIntegrationTest` | Given 트랜잭션 안에서 publish 후 예외로 롤백되는 테스트용 시나리오 / Then `user.registered` 0건 (커밋 후 발행 확인) | REQ-USER-01 |
| T1-18 | 아키텍처 | `ArchitectureTest` (user) | Then R1~R7 통과 (실제 클래스가 생긴 상태에서) | - |
| T1-19 | gateway 단위 | `HeaderSanitizeFilterTest` | Given `X-User-Id: 999`, `x-user-role: ADMIN` / When 필터 / Then 다음 체인이 받은 요청에 두 헤더 없음 | REQ-USER-03 |
| T1-20 | gateway 단위 | `JwtAuthenticationFilterTest` | Given 보호 경로, 토큰 없음 / Then 401 `AUTH_TOKEN_MISSING`, 체인 호출 안 됨 | REQ-USER-03 |
| T1-21 | gateway 단위 | `JwtAuthenticationFilterTest` | Given 만료 토큰 / Then 401 `AUTH_EXPIRED_TOKEN` | REQ-USER-03 |
| T1-22 | gateway 단위 | `JwtAuthenticationFilterTest` | Given 다른 키로 서명한 토큰, 형식 깨진 토큰, 다른 issuer / Then 401 `AUTH_INVALID_TOKEN` | REQ-USER-03 |
| T1-23 | gateway 단위 | `JwtAuthenticationFilterTest` | Given 유효 토큰(sub=42, role=USER) / Then 체인 요청에 `X-User-Id: 42`, `X-User-Role: USER` | REQ-USER-03 |
| T1-24 | gateway 단위 | `JwtAuthenticationFilterTest` | Given 공개 경로 `GET /api/products/drops`, 토큰 없음 / 잘못된 토큰 / Then 체인 호출, 사용자 헤더 없음 | REQ-USER-03 |
| T1-25 | gateway 통합 | `JwtAuthenticationFilterTest` (두 필터를 순서대로 연결) | Given 유효 토큰(sub=42) + 위조 `X-User-Id: 1` / Then 최종 `X-User-Id` = 42 (값 1개) | REQ-USER-03 |
| T1-26 | gateway 단위 | `PublicPathMatcherTest` | `GET /api/rankings/users/weekly` → 공개, `GET /api/rankings/users/me` → 보호, `POST /api/products/drops/1/republish` → 보호, `GET /api/products/drops/1` → 공개 | REQ-USER-03 |
| T1-27 | domain 단위 | `UserTest` | When `User.registerAdmin` / Then role=ADMIN, id=null, 닉네임 규칙 동일 적용 | REQ-DROP-01 (관리자 전제) |
| T1-28 | application 단위 | `UserServiceTest` | Given 빈 저장소 / When `createAdminIfAbsent` 2회 / Then 1회차 true(ADMIN 1건, 해시 저장), 2회차 false, `user.registered` 이벤트 0건 | REQ-DROP-01 |
| T1-29 | application 단위 | `UserServiceTest` | Given 같은 이메일의 USER 계정 존재 / When `createAdminIfAbsent` / Then false, 기존 계정 role=USER 유지 (승격 없음) | REQ-DROP-01 |
| T1-30 | presentation 단위 | `AdminAccountInitializerTest` | Given email 빈 값 / Then `UserService` 호출 없음. Given 비밀번호 7자 / Then `IllegalStateException` | REQ-DROP-01 |
| T1-31 | 통합 | `AdminBootstrapIntegrationTest` (`@SpringBootTest` + PG, `queuedrop.admin.*` 프로퍼티 지정) | When 기동 후 해당 계정으로 login / Then 토큰 role claim = ADMIN, users 에 ADMIN 1행. 컨텍스트 재기동 시에도 1행 | REQ-DROP-01 |

**테스트 지원**
- `TestTokens`(gateway test): 테스트용 secret 으로 jjwt 를 직접 사용해 유효/만료/위조 토큰을 만든다. user-service 코드에 의존하지 않는다.
- 필터 단위 테스트는 `MockServerHttpRequest` + `MockServerWebExchange` 와, 받은 요청을 기록하는 람다 `GatewayFilterChain` 으로 작성한다 (다운스트림 서버 불필요).

---

## 5. 커밋 계획

| # | 커밋 메시지 | 포함 |
|---|-------------|------|
| 1 | `feat(user): 회원 도메인 모델과 저장소 포트 추가` | domain 전체, T1-01~04 |
| 2 | `feat(user): 회원가입·로그인 유스케이스와 외부 연동 포트 추가` | application 전체, Fake 포트, T1-05~09 |
| 3 | `feat(user): users 테이블 마이그레이션과 JPA 영속성 어댑터 추가` | V1 SQL, persistence, T1-10~11 |
| 4 | `feat(user): BCrypt 비밀번호 인코더와 JWT 발급 어댑터 추가` | security, ClockConfig, build.gradle(jjwt, crypto), T1-12~13 |
| 5 | `feat(user): 커밋 후 user.registered 이벤트 발행` | messaging, build.gradle(kafka), yml |
| 6 | `feat(user): 회원가입·로그인 API 추가` | presentation, T1-14~16·17, T1-18 |
| 7 | `feat(user): 기동 시 관리자 계정 생성` | `User.registerAdmin`, `UserService.createAdminIfAbsent`, `presentation/bootstrap/*`, yml, T1-27~31 |
| 8 | `feat(gateway): 클라이언트 사용자 헤더 제거 필터 추가` | HeaderSanitizeFilter, T1-19 |
| 9 | `feat(gateway): JWT 검증 필터와 인증 제외 경로 추가` | auth/*, filter/JwtAuthenticationFilter, config/*, yml, T1-20~26 |
| 10 | `chore: JWT_SECRET·ADMIN 계정 환경변수 예시 추가` | `.env.example` (`JWT_SECRET`, `ADMIN_EMAIL`, `ADMIN_PASSWORD`) |

> 커밋 #2~#4 시점에는 포트 구현 빈(영속성·보안·발행 어댑터)이 아직 모두 있지 않아 `ApplicationContextTest`(T0-03)가 실패한다. 단위·슬라이스 테스트는 각 커밋에서 통과하고, #5 부터 전체가 통과한다 (구현 결정 — 커밋을 계층·어댑터 단위로 나누기 위해 감수).

커밋 본문 예시 (#9):
```
feat(gateway): JWT 검증 필터와 인증 제외 경로 추가

- HS256 서명·만료·발급자 검증 후 X-User-Id, X-User-Role 헤더 주입
- 보호 경로: 토큰 없음/만료/위조 시 401 (AUTH_* 에러 코드)
- 공개 경로: 토큰 상태와 무관하게 통과, 유효 토큰이면 헤더 주입
- 헤더 제거 필터 다음 순서로 실행
```

---

## 6. 결정 필요 · 리스크

### 결정 사항 (확정)
| 항목 | 결정 | 포기한 대안 |
|------|------|-------------|
| 관리자(ADMIN) 계정 생성 | **user-service 기동 시 환경변수(`ADMIN_EMAIL`, `ADMIN_PASSWORD`)로 없으면 생성** (2026-11-16 확정, 3.6 "관리자 계정 초기화") | Flyway 시드 SQL(비밀번호 해시를 SQL 에 고정) / DB 직접 UPDATE |

### 결정 필요
| 항목 | 기본안 | 대안 |
|------|--------|------|
| Access token 만료 | **1시간 — 확정(기본안), 2026-11-16** | 더 짧게 + Refresh token (범위 증가) |

### 설계 수정 필요
| 문서 | 내용 |
|------|------|
| [architecture.md](../../design/02-architecture/architecture.md) 6장 | 인증 제외 경로에 `GET /api/coupons/events/{eventId}`(coupon-issue.md 에서 "공개"), `GET /api/entries/drops/{dropId}/result`(추첨 요약), `/actuator/health` 추가 / "공개 경로에서 잘못된 토큰은 익명 처리" 규칙 명시 → 설계 반영 완료 (2026-11-16) |
| [architecture.md](../../design/02-architecture/architecture.md) 6장 | JWT 세부(iss, 만료 1시간, 비밀키 32바이트 이상, Refresh 없음) 기록 → 설계 반영 완료 (2026-11-16) |
| [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.1 | user-service application 에 `exception/UserErrorCode`, port 의 `TokenSubject`·`IssuedToken`·`UserRegisteredPayload`, domain `exception` 패키지 반영 / gateway 2.6 에 `PublicPathMatcher`, `ErrorResponseWriter`, `GatewayErrorCode` 추가 → 설계 반영 완료 (2026-11-16) |

### 리스크
- **`user.registered` 유실**: 커밋 후 비동기 발행이라 브로커 장애 시 랭킹 닉네임이 빠진다. 랭킹 조회 시 닉네임이 없으면 `"user-{id}"` 로 표시하는 대비를 7단계에 둔다. 확실하게 하려면 Outbox (범위 밖).
- **헤더 신뢰 전제**: 내부 서비스 포트(8081~8086)에 직접 요청하면 `X-User-Id` 를 위조할 수 있다. 로컬 개발에서는 감수하고, 8단계 컨테이너화 때 gateway 만 외부 노출한다 ([infrastructure.md](../../design/03-infrastructure/infrastructure.md) 5장).
- **JWT 비밀키 공유**: 두 서비스 기본값이 다르면 모든 요청이 401. 기본값을 한 곳(`.env.example`)에서 관리하고, 기동 시 키 길이 검증(T1-13)으로 설정 누락을 빨리 드러낸다.
- **JWT_SECRET 기본값**: 로컬 편의를 위해 두 서비스 yml 에 공개된 fallback 값이 있다. 운영에서 환경변수를 빠뜨리면 누구나 토큰을 위조할 수 있으므로, 8단계 컨테이너 실행에서는 fallback 을 쓰지 않는다(프로파일 분리 또는 알려진 기본값이면 기동 실패).
- **로그인 응답 시간**: 이메일이 없을 때도 더미 해시로 비교를 한 번 수행해 응답 시간으로 계정 존재가 드러나지 않게 한다 (E-15, 단계 검증 후 반영).
- **BCrypt 비용**: 로그인·가입마다 약 수십 ms CPU 사용. 부하 테스트 시나리오(8단계)에서는 로그인을 미리 해두고 토큰을 재사용한다.
- **라이브러리 확인**: jjwt 0.13.x API(`verifyWith`, `parseSignedClaims`), Boot 4 의 `JsonMapper` 자동 구성, `spring-boot-starter-webmvc-test` 스타터 이름은 구현 시 확인.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 테스트 ID |
|----------------|-----------|
| 단위: `Email` 형식 검증 | T1-01, T1-02 |
| 단위: 중복 이메일 가입 거부 | T1-06, T1-07, T1-11, T1-15 |
| 단위: 비밀번호 불일치 로그인 실패 | T1-09 |
| 통합: 가입 → DB 저장 + `user.registered` 발행 | T1-16, T1-17 |
| gateway: 토큰 없음/만료/위조 → 401 | T1-20, T1-21, T1-22 |
| gateway: 정상 토큰 → 헤더 주입 | T1-23 |
| gateway: 클라이언트가 보낸 `X-User-Id` 제거 | T1-19, T1-25 |
| (추가) 공개 경로 판단 | T1-24, T1-26 |
| (추가) 계층 규칙 통과 | T1-18 |
| 공통: `./gradlew build` 통과 | 전체 |
| 관리자 계정 생성 (확정 사항 구현) | T1-27 ~ T1-31 |
| 설계 수정 필요 3건 설계 문서 반영 | 6장 |
