package com.queuedrop.gateway.auth;

import org.springframework.http.HttpMethod;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import java.util.List;

/** "METHOD 패턴" 목록으로 인증 제외 경로인지 판단한다 */
@Component
public class PublicPathMatcher {

    private record Rule(HttpMethod method, PathPattern pattern) {}

    private final List<Rule> rules;

    public PublicPathMatcher(AuthProperties props) {
        PathPatternParser parser = PathPatternParser.defaultInstance;
        this.rules = props.auth().publicPaths().stream()
                .map(s -> s.trim().split("\\s+", 2))
                .map(p -> new Rule(HttpMethod.valueOf(p[0]), parser.parse(p[1])))
                .toList();
    }

    public boolean isPublic(HttpMethod method, PathContainer path) {
        return rules.stream().anyMatch(r -> r.method().equals(method) && r.pattern().matches(path));
    }
}
