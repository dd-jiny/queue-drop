package com.queuedrop.common.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * T0-02 보조 케이스(T0-02a): LayerRules R1~R7 이 실제로 위반을 잡는지 common 모듈에서 자동으로 고정한다.
 *
 * - 위반 샘플: sample.bad.r{N}.{변형} 패키지. 변형마다 독립 패키지라 다른 위반이 섞이지 않는다.
 *   변형 이름에는 계층 단어(presentation·application·domain·infrastructure)를 쓰지 않는다.
 *   "..domain.." 같은 상대 패턴이 변형 세그먼트에도 걸려 한 클래스가 두 계층에 속하게 되기 때문이다.
 * - 준수 샘플: sample.good 패키지(4개 계층 아래 9개 하위 패키지). allowEmptyShould(false) 로 검사해
 *   "검사 대상이 비어서 통과"가 아니라 "검사했고 위반이 없어서 통과"임을 보장한다.
 */
class LayerRulesTest {

    private static final String SAMPLE = "com.queuedrop.common.test.archunit.sample";
    private static final JavaClasses GOOD = importSample("good");

    private static JavaClasses importSample(String relativePackage) {
        return new ClassFileImporter().importPackages(SAMPLE + "." + relativePackage);
    }

    private static void assertViolation(ArchRule rule, String ruleId, String samplePackage, String violatingClass) {
        // Given
        JavaClasses classes = importSample(samplePackage);
        assertThat(classes).as("샘플 패키지 %s 가 import 되어야 한다", samplePackage).isNotEmpty();

        // When / Then
        assertThatThrownBy(() -> rule.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(ruleId)
                .hasMessageContaining(violatingClass);
    }

    private static void assertNoViolationAndNotEmpty(ArchRule rule) {
        // Given: GOOD 샘플 / When / Then
        assertThatCode(() -> rule.allowEmptyShould(false).check(GOOD)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("[T0-02a] 준수 샘플은 4개 계층 아래 9개 하위 패키지를 모두 포함한다")
    void goodSampleCoversAllLayers() {
        // Given / When
        List<String> packages = GOOD.stream().map(c -> c.getPackageName().substring(SAMPLE.length() + ".good.".length())).distinct().toList();

        // Then
        assertThat(packages).contains(
                "presentation", "application.service", "application.port", "application.dto", "application.exception",
                "domain.model", "domain.exception", "infrastructure.persistence", "infrastructure.messaging");
    }

    @Test
    @DisplayName("[T0-02a] 계층 패키지가 비어 있으면 allowEmptyShould(true) 로 R1~R7 모두 통과한다 (0단계 전제)")
    void emptyPackagesPass() {
        // Given
        JavaClasses empty = new ClassFileImporter().importPackages(SAMPLE + ".nonexistent");

        // When / Then
        assertThat(empty).isEmpty();
        allRules().forEach(rule -> assertThatCode(() -> rule.check(empty)).doesNotThrowAnyException());
    }

    private static Stream<ArchRule> allRules() {
        return Stream.of(
                LayerRules.R1_domain_depends_only_on_domain_and_java,
                LayerRules.R2_application_does_not_depend_on_outer_layers,
                LayerRules.R3_application_does_not_depend_on_technology,
                LayerRules.R4_presentation_does_not_see_domain,
                LayerRules.R5_infrastructure_does_not_depend_on_presentation,
                LayerRules.R6_application_service_does_not_expose_domain_model,
                LayerRules.R7_jpa_entities_reside_in_persistence);
    }

    @Nested
    class R1 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r1.jpa", "JpaAwareDrop"),                       // jakarta.persistence 어노테이션
                    arguments("bad.r1.toupper", "DropKnowsResult"),            // domain → application
                    arguments("bad.r1.commonerror", "DropThrowsApplicationException"), // domain → common.error
                    arguments("bad.r1.spring", "SpringManagedDomainService"));     // domain → Spring 어노테이션
        }

        @ParameterizedTest(name = "[T0-02a] R1 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R1 domain 이 domain·java 외에 의존하면 실패한다")
        void fails(String sample, String violatingClass) {
            assertViolation(LayerRules.R1_domain_depends_only_on_domain_and_java, "R1:", sample, violatingClass);
        }

        @Test
        @DisplayName("[T0-02a] R1 domain 이 domain·java 만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R1_domain_depends_only_on_domain_and_java);
        }

        @Test
        @DisplayName("[T0-02a] R1 domain.model 에 Lombok @Getter·@RequiredArgsConstructor 를 붙여도 통과한다 (@lombok.Generated 허용)")
        void lombokAllowed() {
            // Given
            JavaClasses classes = importSample("lombok");

            // 전제: 생성 코드에 @lombok.Generated 가 실제로 바이트코드에 남아 있어야 이 테스트가 의미가 있다
            JavaClass drop = classes.get(SAMPLE + ".lombok.domain.model.LombokDrop");
            assertThat(drop.getMethods())
                    .as("Lombok 생성 getter 에 @lombok.Generated 가 남아 있어야 한다")
                    .anyMatch(m -> m.getName().equals("getId") && m.isAnnotatedWith("lombok.Generated"));
            assertThat(drop.getConstructors())
                    .as("Lombok 생성 생성자에 @lombok.Generated 가 남아 있어야 한다")
                    .anyMatch(c -> c.isAnnotatedWith("lombok.Generated"));

            // When / Then
            assertThatCode(() -> LayerRules.R1_domain_depends_only_on_domain_and_java.allowEmptyShould(false).check(classes))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    class R2 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r2.toinfra", "LeakyService"),
                    arguments("bad.r2.topres", "ResponseBuildingService"));
        }

        @ParameterizedTest(name = "[T0-02a] R2 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R2 application 이 presentation·infrastructure 에 의존하면 실패한다")
        void fails(String sample, String violatingClass) {
            assertViolation(LayerRules.R2_application_does_not_depend_on_outer_layers, "R2:", sample, violatingClass);
        }

        @Test
        @DisplayName("[T0-02a] R2 application 이 domain·common.error·spring-context 만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R2_application_does_not_depend_on_outer_layers);
        }
    }

    @Nested
    class R3 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r3.event", "PublishingService"),     // common.event 메시지 record
                    arguments("bad.r3.topics", "TopicAwarePort"),       // common.event 토픽 상수 (port 패키지)
                    arguments("bad.r3.jpa", "EntityManagerService"));   // jakarta.persistence
        }

        @ParameterizedTest(name = "[T0-02a] R3 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R3 application 이 기술·메시지 계약에 의존하면 실패한다")
        void fails(String sample, String violatingClass) {
            assertViolation(LayerRules.R3_application_does_not_depend_on_technology, "R3:", sample, violatingClass);
        }

        @Test
        @DisplayName("[T0-02a] R3 application 이 common.error·spring-context 만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R3_application_does_not_depend_on_technology);
        }
    }

    @Nested
    class R4 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r4.model", "DomainReturningController"),
                    arguments("bad.r4.generic", "GenericDomainController"),       // List<domain.model> 반환
                    arguments("bad.r4.entity", "EntityReturningController"));
        }

        @ParameterizedTest(name = "[T0-02a] R4 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R4 presentation 이 domain·infrastructure 에 의존하면 실패한다")
        void fails(String sample, String violatingClass) {
            assertViolation(LayerRules.R4_presentation_does_not_see_domain, "R4:", sample, violatingClass);
        }

        @Test
        @DisplayName("[T0-02a] R4 presentation 이 application Result 만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R4_presentation_does_not_see_domain);
        }
    }

    @Nested
    class R5 {
        @Test
        @DisplayName("[T0-02a] R5 infrastructure 가 presentation 에 의존하면 실패한다")
        void fails() {
            assertViolation(LayerRules.R5_infrastructure_does_not_depend_on_presentation, "R5:", "bad.r5", "ResponseAwareAdapter");
        }

        @Test
        @DisplayName("[T0-02a] R5 infrastructure 가 application·domain·common.event 만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R5_infrastructure_does_not_depend_on_presentation);
        }
    }

    @Nested
    class R6 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r6.returns", "ExposingService.get()"),          // Drop
                    arguments("bad.r6.param", "ExposingService.save("),            // (Drop)
                    arguments("bad.r6.list", "ExposingService.getAll()"),          // List<Drop>
                    arguments("bad.r6.optional", "ExposingService.find()"),        // Optional<Drop>
                    arguments("bad.r6.nested", "ExposingService.grouped()"),       // Map<Long, List<Drop>>
                    arguments("bad.r6.wildcard", "ExposingService.saveAll("),      // List<? extends Drop>
                    arguments("bad.r6.array", "ExposingService.toArray()"));       // Drop[]
        }

        @ParameterizedTest(name = "[T0-02a] R6 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R6 application.service public 메서드가 domain.model 을 노출하면 실패한다 (제네릭 인자 포함)")
        void fails(String sample, String violatingMethod) {
            assertViolation(LayerRules.R6_application_service_does_not_expose_domain_model, "R6:", sample, violatingMethod);
        }

        @Test
        @DisplayName("[T0-02a] R6 domain.model 을 private·package-private 메서드에서만 쓰면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R6_application_service_does_not_expose_domain_model);
        }

        @Test
        @DisplayName("[T0-02a] R6 public 메서드가 domain.modeling(domain.model 아님) 타입을 노출하면 통과한다 (접두어 오탐 방지)")
        void domainModelingIsNotDomainModel() {
            // Given
            JavaClasses classes = importSample("modeling");
            assertThat(classes.contain(SAMPLE + ".modeling.domain.modeling.DropSnapshot")).isTrue();

            // When / Then
            assertThatCode(() -> LayerRules.R6_application_service_does_not_expose_domain_model.allowEmptyShould(false).check(classes))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    class R7 {
        static Stream<Arguments> violations() {
            return Stream.of(
                    arguments("bad.r7.inmodel", "EntityInDomain"),
                    arguments("bad.r7.injpa", "EntityOutsidePersistence"),  // infrastructure.jpa
                    arguments("bad.r7.inservice", "EntityInApplication"));
        }

        @ParameterizedTest(name = "[T0-02a] R7 위반 샘플 {0} ({1}) 은 실패한다")
        @MethodSource("violations")
        @DisplayName("[T0-02a] R7 @Entity 가 infrastructure.persistence 밖에 있으면 실패한다")
        void fails(String sample, String violatingClass) {
            assertViolation(LayerRules.R7_jpa_entities_reside_in_persistence, "R7:", sample, violatingClass);
        }

        @Test
        @DisplayName("[T0-02a] R7 @Entity 가 infrastructure.persistence 에만 있으면 통과한다")
        void passes() {
            assertNoViolationAndNotEmpty(LayerRules.R7_jpa_entities_reside_in_persistence);
        }
    }
}
