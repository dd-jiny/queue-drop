package com.queuedrop.coupon;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.queuedrop.common.test.archunit.LayerRules;
import com.queuedrop.coupon.application.service.CouponIssueService;
import com.queuedrop.coupon.domain.repository.IssuedCouponRepository;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "com.queuedrop.coupon", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchTests layerRules = ArchTests.in(LayerRules.class);

    /** [T6-21] 발급 응답 경로에서 DB 를 뺀 설계 (단계 문서 3.2): CouponIssueService 는 IssuedCouponRepository 에 의존하지 않는다 */
    @ArchTest
    static final ArchRule issueServiceDoesNotDependOnIssuedCouponRepository = noClasses()
            .that().belongToAnyOf(CouponIssueService.class)
            .should().dependOnClassesThat().areAssignableTo(IssuedCouponRepository.class)
            .because("[T6-21] 발급은 Redis 판정 + coupon.issued 발행만 하고 DB 저장은 컨슈머가 맡는다");
}
