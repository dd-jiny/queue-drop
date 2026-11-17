package com.queuedrop.ranking.support;

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
 * 앱 컨슈머 그룹이 토픽을 끝까지 처리(커밋)했는가 (coupon KafkaConsumption 과 같은 형태).
 * ack-mode=record 라 레코드 처리 후 커밋된다. "점수가 N 이 될 때까지" 만 기다리면 첫 처리 직후 통과해 중복 처리를
 * 검증하지 못하므로 lag 0 을 함께 본다 (T7-21, T7-26).
 */
public final class KafkaConsumption {

    public static final String APP_GROUP = "ranking-service";

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
