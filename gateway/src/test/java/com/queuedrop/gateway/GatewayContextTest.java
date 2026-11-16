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
}
