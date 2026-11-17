package com.queuedrop.user.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * NewTopic(KafkaAdmin) 생성 직후 첫 발행이 producer max.block.ms(1초, application.yml) 안에
 * 토픽 메타데이터를 받지 못해 "Topic ... not present in metadata" 로 실패하는 것을 막는다.
 * 같은 KafkaTemplate(공유 producer)으로 partitionsFor 가 성공할 때까지 기다리면 메타데이터가 캐시된다.
 * 운영 설정(max.block.ms)은 바꾸지 않는다.
 */
public final class KafkaTopicReadiness {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private KafkaTopicReadiness() {}

    public static void awaitTopicMetadata(KafkaTemplate<?, ?> template, String... topics) {
        for (String topic : topics) {
            await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(200)).ignoreExceptions()
                    .until(() -> !template.partitionsFor(topic).isEmpty());
        }
    }
}
