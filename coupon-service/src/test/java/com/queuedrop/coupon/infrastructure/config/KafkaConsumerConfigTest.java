package com.queuedrop.coupon.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * DLT 전용 producer 설정 단위 테스트 (3.3 KafkaConsumerConfig, E-50). 브로커 없이 팩토리 설정만 본다
 * (DefaultKafkaProducerFactory 는 createProducer 전에는 연결하지 않는다).
 * DLT 실제 동작(3회 재시도 후 coupon.issued.DLT, 원본 바이트)은 커밋 #6 T6-23 · T6-29.
 */
class KafkaConsumerConfigTest {

    private DefaultKafkaProducerFactory<String, Object> sharedFactory;
    private KafkaConsumerConfig config;

    @BeforeEach
    void setUp() {
        // application.yml 의 공통 producer 와 같은 핵심 설정 (max.block.ms 1초)
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000");
        sharedFactory = new DefaultKafkaProducerFactory<>(props);
        config = new KafkaConsumerConfig();
    }

    @AfterEach
    void tearDown() {
        sharedFactory.destroy();
    }

    @SuppressWarnings("unchecked")
    private DefaultKafkaProducerFactory<String, Object> dltJsonFactory() {
        return (DefaultKafkaProducerFactory<String, Object>) ReflectionTestUtils.getField(config, "dltJsonFactory");
    }

    @SuppressWarnings("unchecked")
    private DefaultKafkaProducerFactory<String, byte[]> dltBytesFactory() {
        return (DefaultKafkaProducerFactory<String, byte[]>) ReflectionTestUtils.getField(config, "dltBytesFactory");
    }

    private static String maxBlock(DefaultKafkaProducerFactory<?, ?> factory) {
        return String.valueOf(factory.getConfigurationProperties().get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    @DisplayName("[E-50] DLT_MAX_BLOCK_MS 는 5초다")
    void dltMaxBlockConstant() {
        // Then
        assertThat(KafkaConsumerConfig.DLT_MAX_BLOCK_MS).isEqualTo(5_000L);
    }

    @Test
    @DisplayName("[E-50] kafkaErrorHandler 는 DefaultErrorHandler 를 만들고, DLT JSON·bytes 전용 팩토리는 max.block.ms=5000 이다")
    void dltFactoriesUseFiveSecondMaxBlock() {
        // When
        DefaultErrorHandler handler = config.kafkaErrorHandler(sharedFactory);

        // Then
        assertThat(handler).isNotNull();
        assertThat(dltJsonFactory()).isNotNull().isNotSameAs(sharedFactory);
        assertThat(dltBytesFactory()).isNotNull();
        assertThat(maxBlock(dltJsonFactory())).isEqualTo("5000");
        assertThat(maxBlock(dltBytesFactory())).isEqualTo("5000");
    }

    @Test
    @DisplayName("[E-50] DLT 팩토리는 공통 설정을 복사한다 — 공통 producer 의 max.block.ms 1초는 바뀌지 않고, 나머지 설정은 그대로다")
    void sharedFactoryUntouchedAndOtherSettingsCopied() {
        // When
        config.kafkaErrorHandler(sharedFactory);

        // Then
        assertThat(maxBlock(sharedFactory)).isEqualTo("1000");
        assertThat(dltJsonFactory().getConfigurationProperties())
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000")
                .containsEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        assertThat(dltBytesFactory().getConfigurationProperties())
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
    }

    @Test
    @DisplayName("[T6-29 준비] bytes 전용 팩토리는 String 키 · ByteArray 값 직렬화기를 쓴다 (역직렬화 실패 레코드 원본 바이트)")
    void bytesFactoryUsesByteArraySerializer() {
        // When
        config.kafkaErrorHandler(sharedFactory);

        // Then
        assertThat(dltBytesFactory().getKeySerializer()).isInstanceOf(StringSerializer.class);
        assertThat(dltBytesFactory().getValueSerializer()).isInstanceOf(ByteArraySerializer.class);
    }

    @Test
    @DisplayName("[E-50] destroy 는 핸들러 생성 전(팩토리 null)에도, 생성 후에도 예외 없이 끝난다")
    void destroyIsNullSafe() {
        // When / Then
        assertThatCode(() -> new KafkaConsumerConfig().destroy()).doesNotThrowAnyException();
        config.kafkaErrorHandler(sharedFactory);
        assertThatCode(config::destroy).doesNotThrowAnyException();
    }
}
