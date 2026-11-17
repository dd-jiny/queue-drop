package com.queuedrop.coupon.support;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 앱 컨슈머 그룹이 토픽을 끝까지 처리(커밋)했는가 (entry ScenarioTestSupport.entryCreatedFullyConsumed 와 같은 방식).
 * ack-mode=record 라 레코드 처리 후 커밋된다. "DB 건수가 N 이 될 때까지" 만 기다리면 늦게 온 이벤트(유령 등)를
 * 놓칠 수 있어 lag 0 을 함께 본다.
 */
public final class KafkaConsumption {

    public static final String APP_GROUP = "coupon-service";

    private KafkaConsumption() {}

    public static boolean fullyConsumed(KafkaAdmin kafkaAdmin, String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<TopicPartition> partitions = admin.describeTopics(Set.of(topic))
                    .allTopicNames().get().get(topic).partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(
                    partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                    .all().get();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(APP_GROUP)
                    .partitionsToOffsetAndMetadata().get();
            return partitions.stream().allMatch(tp -> {
                long end = ends.get(tp).offset();
                OffsetAndMetadata c = committed.get(tp);
                return end == 0 || (c != null && c.offset() >= end);
            });
        }
    }
}
