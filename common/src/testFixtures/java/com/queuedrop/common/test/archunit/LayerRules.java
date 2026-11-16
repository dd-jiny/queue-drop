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
