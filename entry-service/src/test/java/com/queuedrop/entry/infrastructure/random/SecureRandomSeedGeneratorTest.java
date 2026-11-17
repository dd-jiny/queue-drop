package com.queuedrop.entry.infrastructure.random;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 시드 소스 어댑터 (단계 문서 5단계 3.3, NFR-04). 고정값을 돌려주는 구현이 섞여 들어오지 않았는지만 확인한다 */
class SecureRandomSeedGeneratorTest {

    @Test
    @DisplayName("[T5-01 보강] next() 를 100번 부르면 서로 다른 값이 나온다 (고정 시드 구현 방지)")
    void producesDistinctSeeds() {
        // Given
        SecureRandomSeedGenerator generator = new SecureRandomSeedGenerator();
        Set<Long> seeds = new HashSet<>();

        // When
        for (int i = 0; i < 100; i++) {
            seeds.add(generator.next());
        }

        // Then — 64비트 난수 100개가 충돌할 확률은 무시할 수 있다
        assertThat(seeds).hasSize(100);
    }
}
