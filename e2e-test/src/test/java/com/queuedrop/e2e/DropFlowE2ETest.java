package com.queuedrop.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.MissingNode;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * T5-27 — 드로우 전체 흐름 E2E (phase-5 3.6 시나리오 1~6).
 *
 * <p>인프라(docker compose)와 서비스(gateway·user·product·queue·entry, e2e 프로필)가 떠 있어야 한다.
 * 실행은 {@code scripts/e2e.sh}. 모든 호출은 Gateway 를 거친다 (서비스 모듈에 의존하지 않음).
 *
 * <p>실제 시스템을 대상으로 하므로 시각은 고정 Clock 이 아니라 실제 시각을 쓰고, 대기는 모두 Awaitility 폴링이다.
 * DB 는 실행마다 비우지 않으므로 사용자 이메일·닉네임에 실행 ID 를 붙여 충돌을 피한다.
 */
@Tag("e2e")
class DropFlowE2ETest {

    private static final String GATEWAY_URL = System.getProperty("e2e.gatewayUrl", "http://localhost:8080");
    // user-service application-e2e.yml 의 queuedrop.admin.* 와 같아야 한다
    private static final String ADMIN_EMAIL = System.getProperty("e2e.adminEmail", "e2e-admin@queuedrop.dev");
    private static final String ADMIN_PASSWORD = System.getProperty("e2e.adminPassword", "e2e-admin-password");

    private static final int ENTRY_CAPACITY = 5;   // M
    private static final int WINNER_COUNT = 2;     // K
    private static final int USER_COUNT = 7;

    // product 최소 오픈 지연(Drop.MIN_OPEN_LEAD_TIME = 1분)은 그대로 두고 5초 여유를 둔다 (phase-5 v1.13)
    private static final Duration OPEN_DELAY = Duration.ofSeconds(65);
    private static final Duration ENTRY_PERIOD = Duration.ofSeconds(15);          // closeAt = openAt + 15s
    private static final Duration OPEN_WAIT_TIMEOUT = Duration.ofSeconds(90);     // openAt 까지 기다리는 첫 대기열 진입
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    private final RestClient client = RestClient.builder()
            .baseUrl(GATEWAY_URL)
            .defaultStatusHandler(HttpStatusCode::isError, (req, res) -> { })   // 상태 코드는 테스트가 직접 검증
            .build();

    private final String runId = Long.toString(System.currentTimeMillis(), 36);

    @Test
    @DisplayName("[T5-27] 드로우 등록 → 대기열 → 응모(M=5, 7명) → 마감 → 추첨(K=2) → 결과 조회까지 Gateway 경유로 동작한다")
    void dropFlow() {
        // Given 1. 관리자 로그인 → 상품 등록 → 드로우 등록 (M=5, K=2, openAt=now+65s, closeAt=openAt+15s)
        String adminToken = login(ADMIN_EMAIL, ADMIN_PASSWORD);

        Resp product = call(HttpMethod.POST, "/api/products", adminToken,
                Map.of("name", "E2E 상품 " + runId, "description", "e2e"));
        assertThat(product.status()).as("상품 등록").isEqualTo(201);
        long productId = product.body().path("id").asLong();

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant openAt = now.plus(OPEN_DELAY);
        Instant closeAt = openAt.plus(ENTRY_PERIOD);
        Map<String, Object> dropBody = new LinkedHashMap<>();
        dropBody.put("title", "E2E 드로우 " + runId);
        dropBody.put("entryCapacity", ENTRY_CAPACITY);
        dropBody.put("winnerCount", WINNER_COUNT);
        dropBody.put("openAt", openAt.toString());
        dropBody.put("closeAt", closeAt.toString());
        Resp drop = call(HttpMethod.POST, "/api/products/" + productId + "/drops", adminToken, dropBody);
        assertThat(drop.status()).as("드로우 등록: %s", drop.body()).isEqualTo(201);
        long dropId = drop.body().path("id").asLong();

        // Given 2. 사용자 7명 가입·로그인
        List<String> userTokens = new ArrayList<>();
        for (int i = 0; i < USER_COUNT; i++) {
            String email = "e2e-" + runId + "-" + i + "@queuedrop.dev";
            String password = "e2e-password-" + i;
            Resp signup = call(HttpMethod.POST, "/api/users/signup", null,
                    Map.of("email", email, "password", password, "nickname", "e2e" + runId + i));
            assertThat(signup.status()).as("회원가입 %d: %s", i, signup.body()).isEqualTo(201);
            userTokens.add(login(email, password));
        }

        // When 3. openAt 이후 7명 대기열 진입 → ADMITTED 될 때까지 폴링
        //   오픈 전(409 QUEUE_NOT_OPEN)·drop.created 미반영(404 QUEUE_DROP_NOT_FOUND) 동안만 첫 진입을 재시도하고,
        //   그 밖의 응답은 즉시 실패시킨다 (Awaitility 는 조건 안의 예외를 그대로 던진다)
        String queuePath = "/api/queue/drops/" + dropId;
        await("첫 사용자 대기열 진입").atMost(OPEN_WAIT_TIMEOUT).pollInterval(POLL_INTERVAL).until(() -> {
            Resp enter = call(HttpMethod.POST, queuePath, userTokens.get(0), null);
            if (enter.status() == 200) return true;
            String code = enter.body().path("code").asString();
            boolean retryable = (enter.status() == 409 && "QUEUE_NOT_OPEN".equals(code))
                    || (enter.status() == 404 && "QUEUE_DROP_NOT_FOUND".equals(code));
            if (!retryable) {
                throw new AssertionError("첫 대기열 진입이 재시도 불가 응답으로 실패: status=" + enter.status() + ", body=" + enter.body());
            }
            return false;
        });
        for (int i = 1; i < USER_COUNT; i++) {
            Resp enter = call(HttpMethod.POST, queuePath, userTokens.get(i), null);
            assertThat(enter.status()).as("대기열 진입 %d: %s", i, enter.body()).isEqualTo(200);
        }
        for (int i = 0; i < USER_COUNT; i++) {
            String token = userTokens.get(i);
            await("사용자 " + i + " ADMITTED").atMost(WAIT_TIMEOUT).pollInterval(POLL_INTERVAL)
                    .until(() -> "ADMITTED".equals(call(HttpMethod.GET, queuePath, token, null)
                            .body().path("state").asString()));
        }

        // When 4. 7명 응모 → 5명 202, 2명 409 ENTRY_SOLD_OUT
        String applyPath = "/api/entries/drops/" + dropId;
        List<String> applicantTokens = new ArrayList<>();
        Map<Integer, Integer> statusCounts = new HashMap<>();
        List<String> rejectCodes = new ArrayList<>();
        for (String token : userTokens) {
            Resp apply = call(HttpMethod.POST, applyPath, token, null);
            int status = apply.status();
            statusCounts.merge(status, 1, Integer::sum);
            if (status == 202) {
                applicantTokens.add(token);
            } else {
                rejectCodes.add(apply.body().path("code").asString());
            }
        }
        assertThat(statusCounts).as("응모 응답 분포").containsExactlyInAnyOrderEntriesOf(Map.of(202, 5, 409, 2));
        assertThat(rejectCodes).containsExactly("ENTRY_SOLD_OUT", "ENTRY_SOLD_OUT");

        // When 5. closeAt 까지 기다린 뒤, GET /result 가 200 이 될 때까지 폴링 (closeAt 이후 최대 30초, 공개 API)
        Duration untilClose = Duration.between(Instant.now(), closeAt);
        await("closeAt 도달").atMost(WAIT_TIMEOUT.plus(untilClose.isNegative() ? Duration.ZERO : untilClose))
                .pollInterval(POLL_INTERVAL)
                .until(() -> !Instant.now().isBefore(closeAt));
        String resultPath = "/api/entries/drops/" + dropId + "/result";
        AtomicReference<JsonNode> summary = new AtomicReference<>();
        await("추첨 결과").atMost(WAIT_TIMEOUT).pollInterval(POLL_INTERVAL).until(() -> {
            Resp res = call(HttpMethod.GET, resultPath, null, null);
            if (res.status() != 200) return false;
            summary.set(res.body());
            return true;
        });

        // Then 6. applicantCount=5, winnerCount=2, 응모자 5명의 /me 가 WON 2 · LOST 3, 드로우 상태 DRAWN
        assertThat(summary.get().path("applicantCount").asInt()).isEqualTo(ENTRY_CAPACITY);
        assertThat(summary.get().path("winnerCount").asInt()).isEqualTo(WINNER_COUNT);

        Map<String, Integer> entryStatusCounts = new HashMap<>();
        for (String token : applicantTokens) {
            Resp me = call(HttpMethod.GET, applyPath + "/me", token, null);
            assertThat(me.status()).as("내 응모 조회: %s", me.body()).isEqualTo(200);
            entryStatusCounts.merge(me.body().path("status").asString(), 1, Integer::sum);
        }
        assertThat(entryStatusCounts).containsExactlyInAnyOrderEntriesOf(Map.of("WON", 2, "LOST", 3));

        // product 반영은 draw.completed 비동기 수신이라 폴링
        await("드로우 상태 DRAWN").atMost(WAIT_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> "DRAWN".equals(call(HttpMethod.GET, "/api/products/drops/" + dropId, null, null)
                        .body().path("status").asString()));
    }

    private String login(String email, String password) {
        Resp res = call(HttpMethod.POST, "/api/users/login", null,
                Map.of("email", email, "password", password));
        assertThat(res.status()).as("로그인 %s: %s", email, res.body()).isEqualTo(200);
        return res.body().path("accessToken").asString();
    }

    private Resp call(HttpMethod method, String path, String token, Object body) {
        RestClient.RequestBodySpec spec = client.method(method).uri(path);
        if (token != null) {
            spec.header(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
        if (body != null) {
            spec.contentType(MediaType.APPLICATION_JSON).body(body);
        }
        ResponseEntity<JsonNode> res = spec.retrieve().toEntity(JsonNode.class);
        JsonNode json = res.getBody() == null ? MissingNode.getInstance() : res.getBody();
        return new Resp(res.getStatusCode().value(), json);
    }

    /** 응답 본문이 없어도(예: Gateway 401) body 는 null 이 아니다 — 폴링 중 NPE 방지 */
    private record Resp(int status, JsonNode body) { }
}
