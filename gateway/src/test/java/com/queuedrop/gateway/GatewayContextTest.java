package com.queuedrop.gateway;

import com.queuedrop.gateway.filter.HeaderSanitizeFilter;
import com.queuedrop.gateway.filter.JwtAuthenticationFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.RemoveCachedBodyFilter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.context.ApplicationContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
class GatewayContextTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private List<GlobalFilter> globalFilters;

    @Test
    @DisplayName("[T1-25] 컨텍스트 기동: Boot 4 webflux 에서 JsonMapper(Jackson 3) 빈이 자동 구성된다")
    void jsonMapperAutoConfigured() {
        // Then
        assertThat(context.getBeanProvider(JsonMapper.class).getIfAvailable()).isNotNull();
    }

    @Test
    @DisplayName("[T1-25] 컨텍스트 기동: 두 GlobalFilter 가 등록되고 정렬 시 HeaderSanitizeFilter 가 JwtAuthenticationFilter 보다 먼저, 다른 필터보다 앞서 실행된다")
    void globalFiltersRegisteredInOrder() {
        // Given
        List<GlobalFilter> sorted = new ArrayList<>(globalFilters);

        // When
        AnnotationAwareOrderComparator.sort(sorted);

        // Then
        List<Class<?>> classes = sorted.stream().<Class<?>>map(Object::getClass).toList();
        assertThat(classes).contains(HeaderSanitizeFilter.class, JwtAuthenticationFilter.class);
        int sanitize = classes.indexOf(HeaderSanitizeFilter.class);
        int jwt = classes.indexOf(JwtAuthenticationFilter.class);
        assertThat(sanitize).isLessThan(jwt);
        // 둘보다 앞서는 필터는 없다. 내장 RemoveCachedBodyFilter 가 HIGHEST_PRECEDENCE 동순위지만 헤더를 다루지 않는다.
        assertThat(sorted.subList(0, jwt)).allSatisfy(f ->
                assertThat(f.getClass()).isIn(HeaderSanitizeFilter.class, RemoveCachedBodyFilter.class));
        assertThat(((Ordered) sorted.get(sanitize)).getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE);
        assertThat(((Ordered) sorted.get(jwt)).getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE + 1);
    }

    // ---- 8단계 커밋 #2: Actuator 노출 범위 (phase-8 3.2) ----
    // gateway 의 actuator 는 라우트에 걸리지 않아 GlobalFilter(JWT 인증)를 거치지 않는다. 토큰 없이 호출해 실제 응답을 확인한다.

    @Test
    @DisplayName("[8단계 Actuator] gateway 는 토큰 없이 /actuator/health 와 health 프로브(liveness·readiness)에 200 으로 응답한다")
    void gatewayExposesHealthAndProbes() {
        // Given
        WebTestClient client = WebTestClient.bindToApplicationContext(context).build();

        // When / Then
        client.get().uri("/actuator/health").exchange().expectStatus().isOk();
        client.get().uri("/actuator/health/liveness").exchange().expectStatus().isOk();
        client.get().uri("/actuator/health/readiness").exchange().expectStatus().isOk();
    }

    @Test
    @DisplayName("[8단계 Actuator] gateway 는 외부 공개 포트라 /actuator/prometheus·/actuator/metrics 를 노출하지 않는다 (404)")
    void gatewayDoesNotExposeMetrics() {
        // Given
        WebTestClient client = WebTestClient.bindToApplicationContext(context).build();

        // When / Then
        client.get().uri("/actuator/prometheus").exchange().expectStatus().isNotFound();
        client.get().uri("/actuator/metrics").exchange().expectStatus().isNotFound();
    }
}
