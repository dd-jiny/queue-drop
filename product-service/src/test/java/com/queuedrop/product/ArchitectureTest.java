package com.queuedrop.product;

import com.queuedrop.common.test.archunit.LayerRules;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * [T2-28] 계층 규칙 R1~R7 (layered-architecture.md). 특히 presentation 이 domain(DropStatus 등)을 참조하지 않는다.
 */
@AnalyzeClasses(packages = "com.queuedrop.product", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchTests layerRules = ArchTests.in(LayerRules.class);
}
