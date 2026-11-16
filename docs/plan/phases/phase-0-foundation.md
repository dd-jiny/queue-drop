# 0단계 — 공통 기반 구현 상세

## 변경 이력
| 버전 | 날짜 | 변경 내용 |
|------|------|-----------|
| v1.0 | 2026-11-16 | 최초 작성 |
| v1.1 | 2026-11-16 | Flyway 확정, 공통 producer 설정을 4단계 기준(request 1000 / delivery 3000)으로 통일, DLT 토픽 3종 추가, 설계 반영 완료 표시 |
| v1.2 | 2026-11-16 | 라이브러리 확인 결과 반영: ArchUnit 1.5.1, Testcontainers 2.0.5 (`testcontainers-{module}` 아티팩트명 확인) |
| v1.3 | 2026-11-16 | 3.4 `LayerRules`: R1 에 `lombok..` 허용(설계 v1.2), `isDomainModel` 오탐 방지(`domain.modeling` 제외). 규칙 자체 검증 테스트 `LayerRulesTest` 추가 (T0-02a) |

---

## 1. 개요

| 항목 | 내용 |
|------|------|
| 목표 | 모든 서비스가 같은 규칙(4계층, ArchUnit, 테스트 환경, 이벤트 계약, 에러 형식) 위에서 시작하게 한다 |
| 요구사항 | 직접 대응 REQ 없음 (전 기능의 전제). NFR-01 ~ 03 인프라 전제(Redis AOF, Kafka 설정) |
| 브랜치 | `chore/phase-0-foundation` (`dev` 에서 생성) |
| 선행 조건 | 없음 (초기 골격 커밋 `28c5f40` 이후) |
| 참고 설계 | [layered-architecture.md](../../design/02-architecture/layered-architecture.md), [architecture.md](../../design/02-architecture/architecture.md) 4장(토픽), [infrastructure.md](../../design/03-infrastructure/infrastructure.md) 5장, [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 3장 |

**이 단계가 끝나면**
- 어떤 서비스에서든 계층 규칙을 어기면 `./gradlew build` 가 실패한다.
- 통합 테스트에서 `@Import(PostgresContainerConfig.class)` 한 줄로 실제 PostgreSQL/Redis/Kafka 를 띄울 수 있다.
- `docker compose up -d` 로 AOF 가 켜진 Redis 와 토픽이 미리 생성된 Kafka 가 뜬다.

---

## 2. 산출물 파일 목록

```
queue-drop/
├── build.gradle                                   # 수정: 테스트 의존성, testFixtures 연결
├── docker-compose.yml                             # 수정: Redis AOF, Kafka 토픽 초기화
├── infra/kafka/create-topics.sh                   # 신규
├── common/
│   ├── build.gradle                               # 수정: java-test-fixtures
│   └── src/
│       ├── main/java/com/queuedrop/common/
│       │   ├── event/
│       │   │   ├── Topics.java
│       │   │   ├── UserRegistered.java
│       │   │   ├── DropCreated.java
│       │   │   ├── EntryCreated.java
│       │   │   ├── DrawCompleted.java
│       │   │   └── CouponIssued.java
│       │   └── error/
│       │       ├── ErrorCode.java
│       │       ├── CommonErrorCode.java
│       │       ├── ApplicationException.java
│       │       └── ErrorResponse.java
│       ├── testFixtures/java/com/queuedrop/common/test/
│       │   ├── archunit/LayerRules.java
│       │   └── container/
│       │       ├── PostgresContainerConfig.java
│       │       ├── RedisContainerConfig.java
│       │       └── KafkaContainerConfig.java
│       └── test/java/com/queuedrop/common/test/archunit/
│           ├── LayerRulesTest.java                # T0-02a: 규칙별 위반/준수 샘플로 R1~R7 자체 검증
│           └── sample/**                          # 검증용 샘플 클래스
└── {user,product,queue,entry,coupon,ranking}-service/
    ├── build.gradle                               # 수정: (JPA 서비스) Flyway
    └── src/
        ├── main/java/com/queuedrop/{svc}/
        │   ├── presentation/package-info.java
        │   ├── presentation/api/GlobalExceptionHandler.java
        │   ├── application/package-info.java
        │   ├── domain/package-info.java
        │   └── infrastructure/package-info.java
        ├── main/resources/
        │   ├── application.yml                    # 수정: ddl-auto validate, Flyway, Kafka 직렬화
        │   └── db/migration/.gitkeep              # (JPA 서비스만)
        └── test/java/com/queuedrop/{svc}/
            ├── ArchitectureTest.java
            └── ApplicationContextTest.java        # (JPA 서비스만) 컨테이너 + Flyway 기동 확인
```

---

## 3. 구현 상세

### 3.1 빌드

#### 루트 `build.gradle` (전문)
```groovy
plugins {
    id 'org.springframework.boot' version '4.1.2' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
}

ext {
    set('springCloudVersion', '2025.1.4')
    set('archunitVersion', '1.5.1')
}

allprojects {
    group = 'com.queuedrop'
    version = '0.0.1-SNAPSHOT'

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply plugin: 'java'
    apply plugin: 'io.spring.dependency-management'

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    dependencyManagement {
        imports {
            mavenBom org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES
            mavenBom "org.springframework.cloud:spring-cloud-dependencies:${springCloudVersion}"
        }
    }

    dependencies {
        compileOnly 'org.projectlombok:lombok'
        annotationProcessor 'org.projectlombok:lombok'
        testCompileOnly 'org.projectlombok:lombok'
        testAnnotationProcessor 'org.projectlombok:lombok'
        testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
    }

    tasks.named('test') {
        useJUnitPlatform()
    }
}

// common 을 제외한 모든 모듈은 실행 가능한 Spring Boot 애플리케이션
configure(subprojects.findAll { it.name != 'common' }) {
    apply plugin: 'org.springframework.boot'

    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-actuator'
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
    }
}

// 4계층 서비스: 공통 계층 규칙(ArchUnit)과 Testcontainers 지원을 test fixtures 로 공유
def layeredServices = ['user-service', 'product-service', 'queue-service',
                       'entry-service', 'coupon-service', 'ranking-service']

configure(subprojects.findAll { it.name in layeredServices }) {
    dependencies {
        testImplementation testFixtures(project(':common'))
    }
}
```

#### `common/build.gradle` (전문)
```groovy
apply plugin: 'java-library'
apply plugin: 'java-test-fixtures'

// main: 서비스 간 메시지 계약(event)과 에러 계약(error). 순수 Java — 외부 의존성 없음.
// testFixtures: 계층 규칙(ArchUnit), Testcontainers 설정. 각 서비스 test 에서만 사용.
dependencies {
    testFixturesApi "com.tngtech.archunit:archunit-junit5:${archunitVersion}"

    testFixturesApi 'org.springframework.boot:spring-boot-testcontainers'
    testFixturesApi 'org.springframework.boot:spring-boot-test'
    testFixturesApi 'org.testcontainers:testcontainers-junit-jupiter'
    testFixturesApi 'org.testcontainers:testcontainers-postgresql'
    testFixturesApi 'org.testcontainers:testcontainers-kafka'

    // common 자체 테스트(LayerRulesTest)용. ArchUnit·spring-context 는 testFixturesApi 로 이미 보인다.
    testImplementation 'org.junit.jupiter:junit-jupiter'
    testImplementation 'org.assertj:assertj-core'
    testImplementation 'jakarta.persistence:jakarta.persistence-api'
}
```

> Testcontainers 버전은 Boot BOM 이 관리한다. Boot 4 는 Testcontainers 2.x 를 쓰며 모듈 아티팩트명이 `testcontainers-{module}` 형식으로 바뀌었다 (1.x 의 `postgresql`, `kafka` 아님). 구현 시 `./gradlew :common:dependencies` 로 해석 결과를 확인한다.

#### JPA 서비스 `build.gradle` 추가분 (user, product, entry, coupon)
```groovy
dependencies {
    // ... 기존 의존성 유지
    implementation 'org.springframework.boot:spring-boot-starter-flyway'
    implementation 'org.flywaydb:flyway-database-postgresql'
}
```
(확정: Flyway 도입, `ddl-auto: validate`)

---

### 3.2 common — 이벤트 메시지

**책임**: 서비스 간 메시지 계약. 도메인 모델이 아니다. presentation(Listener) 과 infrastructure(Publisher) 만 직접 사용한다.

```java
package com.queuedrop.common.event;

/** Kafka 토픽 이름. architecture.md 4장과 일치해야 한다. */
public final class Topics {
    public static final String USER_REGISTERED = "user.registered";
    public static final String DROP_CREATED    = "drop.created";
    public static final String ENTRY_CREATED   = "entry.created";
    public static final String DRAW_COMPLETED  = "draw.completed";
    public static final String COUPON_ISSUED   = "coupon.issued";

    /** 저장 컨슈머 실패 시 DefaultErrorHandler 가 3회 재시도 후 보내는 토픽 ({topic}.DLT 규칙) */
    public static final String DROP_CREATED_DLT  = DROP_CREATED + ".DLT";
    public static final String ENTRY_CREATED_DLT = ENTRY_CREATED + ".DLT";
    public static final String COUPON_ISSUED_DLT = COUPON_ISSUED + ".DLT";

    private Topics() {}
}
```

```java
package com.queuedrop.common.event;

/** user-service → ranking. key = userId */
public record UserRegistered(Long userId, String nickname) {}
```

```java
package com.queuedrop.common.event;

import java.time.Instant;

/** product-service → entry, queue, ranking. key = dropId */
public record DropCreated(
        Long dropId,
        Long productId,
        String title,
        int entryCapacity,
        int winnerCount,
        Instant openAt,
        Instant closeAt
) {}
```

```java
package com.queuedrop.common.event;

import java.time.Instant;

/** entry-service → entry(저장), ranking. key = "{dropId}:{userId}" */
public record EntryCreated(
        String eventId,      // UUID
        Long dropId,
        Long userId,
        int entrySeq,
        Instant appliedAt
) {}
```

```java
package com.queuedrop.common.event;

import java.time.Instant;

/** entry-service → product. key = dropId */
public record DrawCompleted(Long dropId, int applicantCount, int winnerCount, Instant drawnAt) {}
```

```java
package com.queuedrop.common.event;

import java.time.Instant;

/** coupon-service → coupon(저장). key = "{couponEventId}:{userId}" */
public record CouponIssued(String eventId, Long couponEventId, Long userId, Instant issuedAt) {}
```

### 3.3 common — 에러 계약

**책임**: 모든 서비스가 같은 에러 응답 형식과 에러 코드 표현을 쓰게 한다. 순수 Java (HTTP 상태도 `int`).

(구현 결정) 에러 코드 인터페이스와 `ApplicationException` 을 `common.error` 에 둔다. application 계층이 `common.error` 에 의존하는 것을 허용하고, `common.event` 의존은 금지한다 (ArchUnit R3 에 반영). → 6장 "설계 수정 필요" 참고.

```java
package com.queuedrop.common.error;

/** 서비스별 에러 코드 enum 이 구현한다 (예: UserErrorCode). */
public interface ErrorCode {
    String code();       // 예: "USER_DUPLICATE_EMAIL"
    int status();        // HTTP 상태 코드
    String message();    // 사용자 노출 기본 메시지
}
```

```java
package com.queuedrop.common.error;

public enum CommonErrorCode implements ErrorCode {
    INVALID_INPUT("COMMON_INVALID_INPUT", 400, "요청 값이 올바르지 않습니다."),
    UNAUTHORIZED("COMMON_UNAUTHORIZED", 401, "인증이 필요합니다."),
    FORBIDDEN("COMMON_FORBIDDEN", 403, "권한이 없습니다."),
    NOT_FOUND("COMMON_NOT_FOUND", 404, "대상을 찾을 수 없습니다."),
    INTERNAL_ERROR("COMMON_INTERNAL_ERROR", 500, "일시적인 오류가 발생했습니다.");

    private final String code;
    private final int status;
    private final String message;

    CommonErrorCode(String code, int status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    @Override public String code() { return code; }
    @Override public int status() { return status; }
    @Override public String message() { return message; }
}
```

```java
package com.queuedrop.common.error;

/** application 계층이 던지는 유일한 비즈니스 예외 타입. presentation 이 HTTP 로 매핑한다. */
public class ApplicationException extends RuntimeException {
    private final ErrorCode errorCode;

    public ApplicationException(ErrorCode errorCode) {
        super(errorCode.message());
        this.errorCode = errorCode;
    }

    public ApplicationException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.message(), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() { return errorCode; }
}
```

```java
package com.queuedrop.common.error;

import java.time.Instant;
import java.util.List;

/** 모든 서비스·게이트웨이 공통 에러 응답 바디. */
public record ErrorResponse(String code, String message, List<FieldError> fieldErrors, Instant timestamp) {

    public record FieldError(String field, String reason) {}

    public static ErrorResponse of(ErrorCode errorCode) {
        return new ErrorResponse(errorCode.code(), errorCode.message(), List.of(), Instant.now());
    }

    public static ErrorResponse of(ErrorCode errorCode, List<FieldError> fieldErrors) {
        return new ErrorResponse(errorCode.code(), errorCode.message(), fieldErrors, Instant.now());
    }
}
```

응답 예시:
```json
{ "code": "USER_DUPLICATE_EMAIL", "message": "이미 가입된 이메일입니다.", "fieldErrors": [], "timestamp": "2026-11-16T09:00:00Z" }
```

### 3.4 common testFixtures — ArchUnit 계층 규칙 R1~R7 (전문)

**책임**: [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 6장 규칙을 코드로 고정한다. 각 서비스는 `ArchTests.in(LayerRules.class)` 로 재사용한다.

```java
package com.queuedrop.common.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.stream.Stream;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * 4계층 의존 규칙. 패키지 패턴은 상대 패턴("..domain..")이며,
 * 분석 범위는 각 서비스의 @AnalyzeClasses(packages = "com.queuedrop.{svc}") 로 제한된다.
 *
 * allowEmptyShould(true): 0단계처럼 계층 패키지가 비어 있어도 규칙이 실패하지 않게 한다.
 *
 * R1 의 "lombok.." 허용: Lombok 이 생성한 메서드에는 CLASS 보존 어노테이션 @lombok.Generated 가 남아
 * 바이트코드 의존으로 잡힌다. 설계 6장 각주(컴파일 타임 Lombok 허용)에 맞추기 위해 허용한다.
 */
public final class LayerRules {

    private static final String PRESENTATION   = "..presentation..";
    private static final String APPLICATION    = "..application..";
    private static final String APP_SERVICE    = "..application.service..";
    private static final String DOMAIN         = "..domain..";
    private static final String DOMAIN_MODEL   = ".domain.model";
    private static final String INFRASTRUCTURE = "..infrastructure..";
    private static final String PERSISTENCE    = "..infrastructure.persistence..";

    private LayerRules() {}

    @ArchTest
    public static final ArchRule R1_domain_depends_only_on_domain_and_java =
            classes().that().resideInAPackage(DOMAIN)
                    .should().onlyDependOnClassesThat().resideInAnyPackage(DOMAIN, "java..", "lombok..")
                    .allowEmptyShould(true)
                    .as("R1: domain 은 domain 과 java.* 외에 의존하지 않는다 (Spring·JPA 금지, Lombok 허용)");

    @ArchTest
    public static final ArchRule R2_application_does_not_depend_on_outer_layers =
            noClasses().that().resideInAPackage(APPLICATION)
                    .should().dependOnClassesThat().resideInAnyPackage(PRESENTATION, INFRASTRUCTURE)
                    .allowEmptyShould(true)
                    .as("R2: application 은 presentation, infrastructure 에 의존하지 않는다");

    @ArchTest
    public static final ArchRule R3_application_does_not_depend_on_technology =
            noClasses().that().resideInAPackage(APPLICATION)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "jakarta.persistence..",
                            "org.springframework.data..",
                            "org.springframework.kafka..",
                            "org.springframework.web..",
                            "com.queuedrop.common.event..")
                    .allowEmptyShould(true)
                    .as("R3: application 은 JPA·Spring Data·Kafka·Web·이벤트 메시지에 의존하지 않는다");

    @ArchTest
    public static final ArchRule R4_presentation_does_not_see_domain =
            noClasses().that().resideInAPackage(PRESENTATION)
                    .should().dependOnClassesThat().resideInAnyPackage(DOMAIN, INFRASTRUCTURE)
                    .allowEmptyShould(true)
                    .as("R4: presentation 은 domain, infrastructure 에 의존하지 않는다 (모델 비노출)");

    @ArchTest
    public static final ArchRule R5_infrastructure_does_not_depend_on_presentation =
            noClasses().that().resideInAPackage(INFRASTRUCTURE)
                    .should().dependOnClassesThat().resideInAPackage(PRESENTATION)
                    .allowEmptyShould(true)
                    .as("R5: infrastructure 는 presentation 에 의존하지 않는다");

    @ArchTest
    public static final ArchRule R6_application_service_does_not_expose_domain_model =
            methods().that().areDeclaredInClassesThat().resideInAPackage(APP_SERVICE)
                    .and().arePublic()
                    .should(notExposeDomainModelInSignature())
                    .allowEmptyShould(true)
                    .as("R6: application.service 의 public 메서드는 domain.model 타입을 파라미터·반환으로 쓰지 않는다");

    @ArchTest
    public static final ArchRule R7_jpa_entities_reside_in_persistence =
            classes().that().areAnnotatedWith("jakarta.persistence.Entity")
                    .should().resideInAPackage(PERSISTENCE)
                    .allowEmptyShould(true)
                    .as("R7: @Entity 는 infrastructure.persistence 에만 존재한다");

    /** 반환 타입·파라미터 타입(제네릭 인자 포함: List<Entry> 등)에 domain.model 타입이 있으면 위반 */
    private static ArchCondition<JavaMethod> notExposeDomainModelInSignature() {
        return new ArchCondition<>("not use domain model types in signature") {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                Stream.concat(Stream.of(method.getReturnType()), method.getParameterTypes().stream())
                        .map(JavaType::getAllInvolvedRawTypes)
                        .flatMap(java.util.Set::stream)
                        .filter(LayerRules::isDomainModel)
                        .forEach(type -> events.add(SimpleConditionEvent.violated(
                                method, method.getFullName() + " exposes domain model " + type.getName())));
            }
        };
    }

    private static boolean isDomainModel(JavaClass type) {
        String pkg = type.getPackageName();
        return pkg.endsWith(DOMAIN_MODEL) || pkg.contains(DOMAIN_MODEL + ".");
    }
}
```

> `getAllInvolvedRawTypes()` 가 제네릭 인자까지 펼쳐준다 (ArchUnit 1.x). `Optional<User>`, `List<Entry>` 반환도 잡힌다.

### 3.5 common testFixtures — Testcontainers 지원

**책임**: 통합 테스트에서 실제 인프라를 띄우고 `@ServiceConnection` 으로 Spring 설정에 자동 연결한다. 컨테이너는 static 이 아니라 Bean 이지만, Spring 테스트 컨텍스트 캐시 덕분에 같은 설정의 테스트끼리는 재사용된다.

```java
package com.queuedrop.common.test.container;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;   // Testcontainers 2.x 패키지, 구현 시 확인

@TestConfiguration(proxyBeanMethods = false)
public class PostgresContainerConfig {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:17");
    }
}
```

```java
package com.queuedrop.common.test.container;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;

@TestConfiguration(proxyBeanMethods = false)
public class RedisContainerConfig {

    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redis() {
        return new GenericContainer<>("redis:7.4").withExposedPorts(6379);
    }
}
```

```java
package com.queuedrop.common.test.container;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;

@TestConfiguration(proxyBeanMethods = false)
public class KafkaContainerConfig {

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.0.0");   // docker-compose 와 같은 이미지
    }
}
```

사용 예:
```java
@SpringBootTest
@Import({PostgresContainerConfig.class, KafkaContainerConfig.class})
class SignupIntegrationTest { /* ... */ }
```

> 테스트 환경에서는 브로커 자동 토픽 생성이 켜져 있으므로(Testcontainers Kafka 기본값) 토픽 초기화 없이 동작한다. 파티션 수에 의존하는 테스트가 필요하면 그 테스트에서 `AdminClient` 로 직접 생성한다.

### 3.6 서비스 공통 — 패키지 골격

각 서비스(user, product, queue, entry, coupon, ranking)에 4개 계층 패키지를 만든다. 빈 패키지는 git 에 남지 않으므로 `package-info.java` 로 고정하고 계층 책임을 주석으로 적는다.

```java
/**
 * presentation 계층: 입력 어댑터 (Controller, Kafka Listener, Scheduler).
 * Request → Command, Result → Response 변환만 한다. domain 을 import 하지 않는다.
 */
package com.queuedrop.entry.presentation;
```
```java
/** application 계층: 유스케이스(service), Command/Result(dto), 외부 연동 포트(port). */
package com.queuedrop.entry.application;
```
```java
/** domain 계층: 모델(model), 도메인 서비스(service), 영속성 포트(repository). 순수 Java. */
package com.queuedrop.entry.domain;
```
```java
/** infrastructure 계층: persistence, redis, messaging, config. 포트를 구현한다. */
package com.queuedrop.entry.infrastructure;
```

하위 패키지(`presentation/api`, `application/service` 등)는 실제 클래스가 생기는 단계에서 만든다.

### 3.7 서비스 공통 — 아키텍처 테스트

```java
package com.queuedrop.entry;

import com.queuedrop.common.test.archunit.LayerRules;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

@AnalyzeClasses(packages = "com.queuedrop.entry", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchTests layerRules = ArchTests.in(LayerRules.class);
}
```
서비스마다 `packages` 값만 바꾼다. gateway 는 계층 구조를 적용하지 않으므로([class-diagram.md](../../design/07-class-diagram/class-diagram.md) 2.6) 제외.

#### 규칙이 실제로 동작하는지 확인하는 방법 (완료 조건)
1. entry-service 에 임시 클래스 2개를 추가한다 (커밋하지 않음).
   ```java
   // src/main/java/com/queuedrop/entry/domain/model/Dummy.java
   package com.queuedrop.entry.domain.model;
   public record Dummy(Long id) {}

   // src/main/java/com/queuedrop/entry/presentation/api/ViolationController.java
   package com.queuedrop.entry.presentation.api;
   import com.queuedrop.entry.domain.model.Dummy;
   public class ViolationController { public Dummy leak() { return new Dummy(1L); } }
   ```
2. `./gradlew :entry-service:test --tests '*ArchitectureTest'` → **R4 위반으로 실패**하는지 확인.
3. 같은 방식으로 `application/service` 에 `public Dummy find()` 를 만들어 **R6 실패**, `domain` 클래스에서 `org.springframework.stereotype.Component` 를 붙여 **R1 실패**를 확인.
4. 임시 클래스를 삭제하고 테스트가 다시 통과하는지 확인.

### 3.8 서비스 공통 — 전역 예외 처리기

**책임**: application 이 던진 `ApplicationException` 과 요청 검증 실패를 공통 `ErrorResponse` 로 매핑한다. presentation 계층에 둔다.

(구현 결정) Spring Web 의존이 필요하므로 `common`(순수 Java)에 두지 않고 서비스마다 같은 클래스를 둔다. 서비스 6개 × 1 클래스로 중복이 작아 별도 모듈을 만들지 않는다.

```java
package com.queuedrop.entry.presentation.api;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.common.error.ErrorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ApplicationException.class)
    public ResponseEntity<ErrorResponse> handleApplication(ApplicationException e) {
        ErrorCode code = e.errorCode();
        if (code.status() >= 500) {
            log.error("application error: {}", code.code(), e);
        }
        return ResponseEntity.status(code.status()).body(ErrorResponse.of(code));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        List<ErrorResponse.FieldError> fields = e.getBindingResult().getFieldErrors().stream()
                .map(f -> new ErrorResponse.FieldError(f.getField(), f.getDefaultMessage()))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of(CommonErrorCode.INVALID_INPUT, fields));
    }

    /** Gateway 를 거치지 않아 X-User-Id 가 없는 요청 */
    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException e) {
        return ResponseEntity.status(401).body(ErrorResponse.of(CommonErrorCode.UNAUTHORIZED));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e) {
        log.error("unexpected error", e);
        return ResponseEntity.internalServerError().body(ErrorResponse.of(CommonErrorCode.INTERNAL_ERROR));
    }
}
```

### 3.9 서비스 공통 — application.yml

#### JPA 서비스 (user, product, entry, coupon) — 예: user-service 전문
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
      ddl-auto: validate        # 스키마는 Flyway 가 만든다. 엔티티와 불일치 시 기동 실패
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: false
```
product / entry / coupon 은 포트·이름·DB 명만 다르다 (기존 값 유지).

#### Kafka 서비스 (product, queue, entry, coupon, ranking) — `spring.kafka` 공통 블록
```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer   # Spring Kafka 4 (Jackson 3), 구현 시 확인
      acks: all
      retries: 2147483647          # 횟수가 아니라 delivery.timeout.ms 로 전체 시간을 제한
      properties:
        enable.idempotence: true
        request.timeout.ms: 1000
        delivery.timeout.ms: 3000
    consumer:
      group-id: ${spring.application.name}
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
        spring.json.trusted.packages: com.queuedrop.common.event
```
- `acks: all`, `enable.idempotence: true` 는 [D2](../../design/04-non-functional/concurrency-consistency.md) "성공 응답 = 이벤트 기록" 의 전제.
- **동기 발행 정책 (확정, 응모·쿠폰 공통)**: 애플리케이션 재시도 없음. producer 내부 재시도(멱등)만 사용하고 `request.timeout.ms=1000`, `delivery.timeout.ms=3000` 으로 send 결과가 3초 안에 확정되게 한다. 호출 측 `send().get()` 대기는 3.5초, 최종 실패 시 보상. 근거는 [phase-4-entry-apply.md](phase-4-entry-apply.md) 3장 (애플리케이션이 다시 send 하면 멱등 producer 도 별개 레코드로 취급해 유령 이벤트가 생긴다).
- 저빈도 이벤트를 보내는 서비스(user, product)는 발행 대기 시간이 응답에 포함되지 않으므로 `delivery.timeout.ms` 를 서비스별로 늘려도 된다 (각 단계 문서 참고).
- 토픽 하나에 한 종류 메시지만 흐르므로, 컨슈머 쪽 타입은 각 `@KafkaListener` 의 파라미터 타입 / 타입 헤더로 결정된다. 타입 헤더 매핑 방식은 2단계 첫 Listener 구현 시 확정한다.
- user-service 는 1단계에서 Kafka 의존성을 추가할 때 이 블록을 넣는다 (현재 골격에 Kafka 없음).

### 3.10 인프라 — docker-compose.yml (전문)

```yaml
services:
  postgres:
    image: postgres:17
    container_name: queuedrop-postgres
    environment:
      POSTGRES_USER: queuedrop
      POSTGRES_PASSWORD: queuedrop
      POSTGRES_DB: queuedrop
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./infra/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U queuedrop"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7.4
    container_name: queuedrop-redis
    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 10

  kafka:
    image: apache/kafka:4.0.0
    container_name: queuedrop-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      retries: 10

  kafka-init:
    image: apache/kafka:4.0.0
    container_name: queuedrop-kafka-init
    depends_on:
      kafka:
        condition: service_healthy
    volumes:
      - ./infra/kafka/create-topics.sh:/create-topics.sh:ro
    entrypoint: ["/bin/bash", "/create-topics.sh"]
    restart: "no"

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: queuedrop-kafka-ui
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: queue-drop
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      kafka:
        condition: service_healthy

volumes:
  postgres-data:
  redis-data:
```

#### `infra/kafka/create-topics.sh` (전문)
```bash
#!/bin/bash
# 토픽 사전 생성. 여러 번 실행해도 안전하다(--if-not-exists).
set -euo pipefail

BOOTSTRAP="kafka:29092"
TOPICS_BIN="/opt/kafka/bin/kafka-topics.sh"

create() {
  local topic=$1 partitions=$2; shift 2
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
  echo "created: $topic (partitions=$partitions $*)"
}

# 저빈도 이벤트: 파티션 1
create user.registered 1
create drop.created    1
create draw.completed  1

# 피크 이벤트: 컨슈머 병렬 처리를 위해 파티션 6
# entry.created 는 랭킹 재집계를 위해 30일 보존 (30 * 24 * 60 * 60 * 1000 ms)
create entry.created  6 --config retention.ms=2592000000
create coupon.issued  6

# DLT: 저장 컨슈머(entry, coupon)가 3회 재시도 후 보내는 토픽. 재처리용이라 파티션 1 (구현 결정)
create drop.created.DLT  1
create entry.created.DLT 1
create coupon.issued.DLT 1

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list
```

(구현 결정) 파티션 수: 피크 토픽 6, 저빈도 토픽 1. 근거는 8단계 부하 테스트에서 검증하고 조정한다.

---

## 4. 테스트 케이스

| ID | 계층/종류 | 테스트 클래스 | 시나리오 (Given / When / Then) | 관련 REQ |
|----|-----------|---------------|--------------------------------|----------|
| T0-01 | 아키텍처 | `{svc}.ArchitectureTest` ×6 | Given 4계층 골격만 있는 서비스 / When ArchUnit 실행 / Then R1~R7 모두 통과 (빈 패키지 허용) | - |
| T0-02a | 아키텍처 (자동) | common `LayerRulesTest` | Given 규칙별 위반/준수 샘플 클래스 / When R1~R7 check / Then 위반 샘플은 실패, 준수 샘플은 `allowEmptyShould(false)` 로도 통과. R3 위반 샘플은 `jakarta.persistence`·`common.event` 만 (Spring Data·Kafka·Web 은 common 에 의존성이 없어 범위 밖). 서비스 패키지 오타는 잡지 못하므로 T0-02 는 계속 필요 | - |
| T0-02 | 아키텍처 (수동) | entry-service `ArchitectureTest` | Given presentation 이 domain 모델을 반환하는 임시 클래스 / When 실행 / Then R4 실패. 같은 방식으로 R6, R1 실패 확인 후 임시 클래스 삭제 (3.7) | - |
| T0-03 | 통합 | `{svc}.ApplicationContextTest` ×4 (user, product, entry, coupon) | Given `PostgresContainerConfig` / When 컨텍스트 기동 / Then Flyway 실행 + `ddl-auto: validate` 통과, 기동 성공 | - |
| T0-04 | 통합 | `{svc}.ApplicationContextTest` (queue, ranking) | Given `RedisContainerConfig`, `KafkaContainerConfig` / When 기동 / Then 성공 | - |
| T0-05 | 단위 | presentation `GlobalExceptionHandlerTest` (entry-service 1곳) | Given `ApplicationException(CommonErrorCode.NOT_FOUND)` 를 던지는 테스트 컨트롤러 / When 호출 / Then 404 + `code=COMMON_NOT_FOUND` / 검증 실패 시 400 + fieldErrors | - |
| T0-06 | 인프라 (수동) | - | Given `docker compose up -d` / When `docker compose ps` / Then postgres·redis·kafka healthy, kafka-init exit 0 | NFR-03 |
| T0-07 | 인프라 (수동) | - | When `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic entry.created` / Then PartitionCount 6, `retention.ms=2592000000` | REQ-RANK-03 |
| T0-09 | 인프라 (수동) | - | When `kafka-topics.sh --list` / Then 토픽 8개(이벤트 5 + DLT 3: `drop.created.DLT`, `entry.created.DLT`, `coupon.issued.DLT`) | NFR-03 |
| T0-08 | 인프라 (수동) | - | Given `redis-cli SET t 1` / When `docker compose restart redis` 후 `GET t` / Then `1` 유지, `CONFIG GET appendonly` = yes | NFR-01 |

---

## 5. 커밋 계획

| # | 커밋 메시지 | 포함 파일 |
|---|-------------|-----------|
| 1 | `chore: 테스트 의존성과 common test fixtures 설정` | 루트 `build.gradle`, `common/build.gradle` |
| 2 | `feat(common): 서비스 간 이벤트 메시지 스키마와 토픽 상수 추가` | `common/.../event/*` |
| 3 | `feat(common): 공통 에러 코드와 에러 응답 형식 추가` | `common/.../error/*` |
| 4 | `test(common): ArchUnit 4계층 의존 규칙 R1~R7 추가` | `LayerRules.java`, `LayerRulesTest.java`, `sample/**`, `common/build.gradle`(testImplementation) |
| 5 | `test(common): Testcontainers 지원 설정 추가` | `container/*Config.java` |
| 6 | `chore: 서비스별 4계층 패키지 골격과 아키텍처 테스트 추가` | 각 서비스 `package-info.java`, `ArchitectureTest.java` |
| 7 | `feat: 서비스별 전역 예외 처리기 추가` | 각 서비스 `GlobalExceptionHandler.java`, T0-05 |
| 8 | `chore: Flyway 도입 및 JPA ddl-auto validate 전환` | JPA 서비스 `build.gradle`, `application.yml`, `db/migration/.gitkeep`, `ApplicationContextTest` |
| 9 | `chore: Kafka 프로듀서·컨슈머 공통 설정 추가` | Kafka 서비스 `application.yml` |
| 10 | `chore(infra): Redis AOF 활성화 및 Kafka 토픽·DLT 토픽 사전 생성` | `docker-compose.yml`, `infra/kafka/create-topics.sh` |

커밋 본문 예시 (#4):
```
test(common): ArchUnit 4계층 의존 규칙 R1~R7 추가

- domain 순수성, 계층 간 의존 방향, 모델 비노출(시그니처 수준) 규칙
- 빈 계층 패키지에서도 실패하지 않도록 allowEmptyShould 적용
- 각 서비스는 ArchTests.in(LayerRules.class) 로 재사용
```

---

## 6. 결정 필요 · 리스크

### 결정 사항 (확정)
| 항목 | 결정 | 포기한 대안 |
|------|------|-------------|
| DB 스키마 관리 | **Flyway 도입, `ddl-auto: validate`** (2026-11-16 확정) | `ddl-auto: update` 유지 — 엔티티 분리 구조에서 스키마 변경 이력이 남지 않음 |
| Kafka 동기 발행 | **producer 내부 재시도 + `delivery.timeout.ms=3000`, 애플리케이션 재시도 없음** (2026-11-16 확정) | `get(3s)` + 애플리케이션 재시도 2회 |
| DLT | 저장 컨슈머(entry, coupon)는 3회 재시도 후 `{topic}.DLT`. 랭킹은 로그·스킵 (2026-11-16 확정) | 전 컨슈머 로그·스킵 |

### 설계 수정 필요
| 문서 | 내용 |
|------|------|
| [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 3장 | application 의존 허용 목록에 **`common.error`(ErrorCode, ApplicationException)** 추가. 4.5 의 "application 이 에러 코드 정의"는 유지 (서비스별 enum 이 `ErrorCode` 구현) → 설계 반영 완료 (2026-11-16) |
| [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 6장 | R3 금지 목록에 `org.springframework.web..`, `com.queuedrop.common.event..` 추가 (4.4 "application 은 common 메시지에 직접 의존하지 않는다"를 규칙화) → 설계 반영 완료 (2026-11-16) |
| [layered-architecture.md](../../design/02-architecture/layered-architecture.md) 5장 | domain 하위 패키지에 `exception`(도메인 예외) 추가 — 4.5 에서 `DomainException` 을 언급하지만 패키지 구조에 없음 → 설계 반영 완료 (2026-11-16) |
| [class-diagram.md](../../design/07-class-diagram/class-diagram.md) 3장 | `DropCreated` 에 `productId` 필드 누락 — [architecture.md](../../design/02-architecture/architecture.md) 4장 페이로드에는 있음. 이 문서는 architecture.md 기준으로 `productId` 포함 → 설계 반영 완료 (2026-11-16) |

### 리스크
- **라이브러리 좌표 불확실**: Testcontainers 2.x 아티팩트·패키지명, Spring Kafka 4 의 Jackson 3 직렬화 클래스명(`JacksonJsonSerializer`)은 구현 시 실제 해석 결과로 확인한다. 다르면 이 문서의 코드 블록을 수정한다.
- **ArchUnit 빈 패키지**: `allowEmptyShould(true)` 때문에 패키지 이름을 잘못 지으면(예: `presentaion`) 규칙이 아무것도 검사하지 않고 통과한다. T0-02 수동 확인을 반드시 한다.
- **Testcontainers 실행 시간**: 서비스마다 컨테이너를 띄우므로 전체 `./gradlew build` 시간이 늘어난다. 필요하면 Testcontainers reuse(`~/.testcontainers.properties` 의 `testcontainers.reuse.enable=true`)를 로컬에서만 켠다.
- **로컬 compose 와 테스트 환경 차이**: 로컬 Kafka 는 자동 토픽 생성 off, 테스트 컨테이너는 on. "토픽이 없어서 실패"하는 문제는 로컬에서만 드러날 수 있다 → 로컬 기동 시 kafka-init 완료를 먼저 확인.

---

## 7. 완료 체크리스트

| 플랜 완료 조건 | 확인 방법 |
|----------------|-----------|
| 모든 모듈 `./gradlew build` 통과, 각 서비스 `ArchitectureTest` 실행됨 | T0-01, T0-03, T0-04, T0-05 |
| 계층 규칙을 일부러 어기면 테스트가 실패함 | T0-02 |
| `docker compose up -d` 후 모든 컨테이너 healthy, 토픽 5개 + DLT 3개 생성 | T0-06, T0-07, T0-09 |
| (추가) Redis AOF 동작 | T0-08 |
| 결정 사항(Flyway) 확정 및 [implementation-plan.md](../implementation-plan.md) 4장 반영 — 완료 | 6장 |
| 설계 수정 필요 항목 4건 설계 문서 반영 (변경 이력 추가) | 6장 |
