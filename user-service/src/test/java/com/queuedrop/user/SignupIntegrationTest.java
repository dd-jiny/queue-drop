package com.queuedrop.user;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.event.Topics;
import com.queuedrop.common.test.container.KafkaContainerConfig;
import com.queuedrop.common.test.container.PostgresContainerConfig;
import com.queuedrop.user.application.port.UserEventPublisher;
import com.queuedrop.user.application.port.UserRegisteredPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@AutoConfigureMockMvc
@Import({PostgresContainerConfig.class, KafkaContainerConfig.class})
class SignupIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration QUIET_PERIOD = Duration.ofSeconds(3);

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired UserEventPublisher eventPublisher;
    @Autowired KafkaContainer kafka;

    private KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "signup-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(Topics.USER_REGISTERED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    /** 지금까지 받은 메시지 중 key 가 일치하는 것. 호출할 때마다 한 번 poll 해 누적한다. */
    private List<ConsumerRecord<String, String>> recordsWithKey(String key) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> key.equals(r.key())).toList();
    }

    private List<ConsumerRecord<String, String>> recordsWithNickname(String nickname) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream().filter(r -> JSON.readTree(r.value()).path("nickname").asString("").equals(nickname)).toList();
    }

    private MvcResult signup(String email, String nickname) throws Exception {
        return mockMvc.perform(post("/api/users/signup").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"%s","password":"password123","nickname":"%s"}""".formatted(email, nickname)))
                .andReturn();
    }

    private int countUsers(String email) {
        return jdbcTemplate.queryForObject("select count(*) from users where email = ?", Integer.class, email);
    }

    @Test
    @DisplayName("[T1-16] 가입하면 201, users 1행, user.registered 에 key=userId, 값 {userId, nickname} 1건이 발행된다")
    void signupPersistsAndPublishes() throws Exception {
        // Given
        String email = "it-" + UUID.randomUUID() + "@example.com";

        // When
        MvcResult result = signup(email, "signup-it");

        // Then: 응답과 DB
        assertThat(result.getResponse().getStatus()).isEqualTo(201);
        long userId = JSON.readTree(result.getResponse().getContentAsString()).path("userId").asLong();
        assertThat(countUsers(email)).isEqualTo(1);

        // Then: 메시지
        String key = String.valueOf(userId);
        await().atMost(Duration.ofSeconds(15)).until(() -> !recordsWithKey(key).isEmpty());
        List<ConsumerRecord<String, String>> records = recordsWithKey(key);
        assertThat(records).hasSize(1);
        JsonNode value = JSON.readTree(records.getFirst().value());
        assertThat(value.path("userId").asLong()).isEqualTo(userId);
        assertThat(value.path("nickname").asString()).isEqualTo("signup-it");
    }

    @Test
    @DisplayName("[T1-16] 같은 이메일로 다시 가입하면 409 USER_DUPLICATE_EMAIL, users 는 1행, 이벤트는 추가되지 않는다")
    void duplicateSignupReturns409() throws Exception {
        // Given
        String email = "dup-" + UUID.randomUUID() + "@example.com";
        String nickname = "dup-" + UUID.randomUUID().toString().substring(0, 8);
        assertThat(signup(email, nickname).getResponse().getStatus()).isEqualTo(201);
        await().atMost(Duration.ofSeconds(15)).until(() -> recordsWithNickname(nickname).size() == 1);

        // When
        mockMvc.perform(post("/api/users/signup").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"%s","password":"password123","nickname":"%s"}""".formatted(email.toUpperCase(), nickname)))
                // Then
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("USER_DUPLICATE_EMAIL"));
        assertThat(countUsers(email)).isEqualTo(1);
        await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(2))
                .until(() -> recordsWithNickname(nickname).size() == 1);
    }

    @Test
    @DisplayName("[T1-17] 트랜잭션 안에서 발행 요청 후 롤백되면 user.registered 는 발행되지 않는다 (커밋된 경우는 발행된다)")
    void publishOnlyAfterCommit() {
        // Given
        String rolledBackKey = "900000001";
        String committedKey = "900000002";

        // When: 발행 요청 후 예외로 롤백
        try {
            transactionTemplate.executeWithoutResult(tx -> {
                eventPublisher.publishRegistered(new UserRegisteredPayload(Long.valueOf(rolledBackKey), "rollback"));
                throw new IllegalStateException("force rollback");
            });
        } catch (IllegalStateException expected) {
            // 롤백 유도
        }
        // When: 대조군 — 같은 방식으로 커밋
        transactionTemplate.executeWithoutResult(tx ->
                eventPublisher.publishRegistered(new UserRegisteredPayload(Long.valueOf(committedKey), "commit")));

        // Then: 커밋된 쪽은 도착한다 (컨슈머·브로커가 정상임을 확인)
        await().atMost(Duration.ofSeconds(15)).until(() -> recordsWithKey(committedKey).size() == 1);
        // Then: 롤백된 쪽은 대기 시간 동안 끝내 도착하지 않는다
        await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(2))
                .until(() -> recordsWithKey(rolledBackKey).isEmpty());
    }
}
