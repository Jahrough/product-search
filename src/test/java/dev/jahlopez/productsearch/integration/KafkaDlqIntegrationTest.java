package dev.jahlopez.productsearch.integration;

import com.redis.testcontainers.RedisContainer;
import dev.jahlopez.productsearch.config.KafkaTestProducerConfig;
import dev.jahlopez.productsearch.event.ProductEvent;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.testcontainers.OpensearchContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import({KafkaTestProducerConfig.class, KafkaDlqIntegrationTest.ShortBackOffConfig.class})
class KafkaDlqIntegrationTest {

    /** Keeps the production error-handler wiring but replaces the 120s retry budget with a short one. */
    @org.springframework.boot.test.context.TestConfiguration(proxyBeanMethods = false)
    static class ShortBackOffConfig {
        @org.springframework.context.annotation.Bean
        @org.springframework.context.annotation.Primary
        org.springframework.util.backoff.BackOff testRetryBackOff() {
            return new org.springframework.util.backoff.FixedBackOff(200L, 3L);
        }
    }

    private static final String MAIN_TOPIC = "product-events";
    private static final String DLT_TOPIC = MAIN_TOPIC + ".DLT";

    private static final String POISON_SKU = "POISON-PILL-SKU";
    private static final Long POISON_PRODUCT_ID = 99001L;

    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration DLT_POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration DLT_DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration LISTENER_ASSIGNMENT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    // Infrastructure shared by Spring's application context and this test.
    @Container
    static final RedisContainer redis = new RedisContainer(DockerImageName.parse("redis:7-alpine"));

    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.0");

    @Container
    static final OpensearchContainer<?> opensearch = new OpensearchContainer<>(DockerImageName.parse("opensearchproject/opensearch:2.11.0"));

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("postgres:15-alpine"));

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @DynamicPropertySource
    static void configureContainerProperties(DynamicPropertyRegistry registry) {
        // Redis
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        // PostgreSQL
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Kafka
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.key-serializer", () -> StringSerializer.class.getName());
        registry.add("spring.kafka.producer.value-serializer", () -> JacksonJsonSerializer.class.getName());
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");

        // OpenSearch
        registry.add("app.opensearch.uris", opensearch::getHttpHostAddress);
    }

    /**
     * Waits for the application's Kafka listener containers to receive
     * their expected partition assignments before each test.
     *
     * <p>The expected assignment count must match the number of partitions
     * assigned to each listener container in the test broker.
     */
    @BeforeEach
    void waitForApplicationListeners() {
        for (MessageListenerContainer container
                : listenerRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    @Test
    @DisplayName("Publishes a repeatedly failing product event to the DLT")
    void shouldRouteFailedEventToDeadLetterTopic() throws Exception {
        ProductEvent event = createFailingProductEvent();

        // Subscribe before publishing to avoid missing an early DLT record.
        try (KafkaConsumer<String, byte[]> dltConsumer = createDltConsumer()) {
            subscribeAndAwaitAssignment(dltConsumer);

            kafkaTemplate.send(MAIN_TOPIC, event.sku(), event)
                    .get(SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            final List<ConsumerRecord<String, byte[]>> receivedRecords = new ArrayList<>();

            await()
                    .alias("event to arrive on " + DLT_TOPIC)
                    .atMost(DLT_DELIVERY_TIMEOUT)
                    .pollInterval(POLL_INTERVAL)
                    .untilAsserted(() -> {
                        dltConsumer.poll(DLT_POLL_TIMEOUT)
                                .records(DLT_TOPIC)
                                .forEach(receivedRecords::add);

                        assertThat(receivedRecords)
                                .filteredOn(record -> event.sku().equals(record.key()))
                                .anySatisfy(record -> {
                                    String payload = new String(
                                            record.value(),
                                            StandardCharsets.UTF_8
                                    );
                                    assertThat(payload)
                                            .contains(event.productId().toString())
                                            .contains(event.sku())
                                            .contains(event.eventId().toString());
                                });
                    });
        }
    }

    @Test
    @DisplayName("Preserves original bytes for deserialization failures in the DLT")
    void shouldPreserveOriginalPayloadBytesForDeserializationFailure() throws Exception {
        String key = POISON_SKU + "-" + UUID.randomUUID();
        byte[] originalPayload = {
                0x00, (byte) 0xFF, (byte) 0x80, '{', 'x', ':', 0x01
        };

        try (KafkaConsumer<String, byte[]> dltConsumer = createDltConsumer()) {
            subscribeAndAwaitAssignment(dltConsumer);
            sendRawPayload(key, originalPayload);

            List<ConsumerRecord<String, byte[]>> receivedRecords = new ArrayList<>();

            await()
                    .alias("original bytes to arrive on " + DLT_TOPIC)
                    .atMost(DLT_DELIVERY_TIMEOUT)
                    .pollInterval(POLL_INTERVAL)
                    .untilAsserted(() -> {
                        dltConsumer.poll(DLT_POLL_TIMEOUT)
                                .records(DLT_TOPIC)
                                .forEach(receivedRecords::add);

                        assertThat(receivedRecords)
                                .filteredOn(record -> key.equals(record.key()))
                                .anySatisfy(record ->
                                        assertThat(record.value())
                                                .containsExactly(originalPayload)
                                );
                    });
        }
    }

    private void sendRawPayload(String key, byte[] payload) throws Exception {
        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()
        );

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                new ByteArraySerializer()
        )) {
            producer.send(new ProducerRecord<>(MAIN_TOPIC, key, payload))
                    .get(SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
    }

    /**
     * Creates an independent consumer for inspecting the DLT.
     *
     * <p>A unique group ID avoids inheriting committed offsets from another
     * test run. Byte-array deserialization allows assertions against the
     * exact bytes published to the DLT.
     */
    private KafkaConsumer<String, byte[]> createDltConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    /**
     * Subscribes the DLT consumer and waits until Kafka assigns a partition.
     */
    private void subscribeAndAwaitAssignment(
            KafkaConsumer<String, byte[]> consumer) {
        consumer.subscribe(List.of(DLT_TOPIC));

        await()
                .alias("DLT consumer partition assignment")
                .atMost(LISTENER_ASSIGNMENT_TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .until(() -> {
                    consumer.poll(DLT_POLL_TIMEOUT);
                    return !consumer.assignment().isEmpty();
                });
    }

    /**
     * Builds the event that the application listener is configured to reject.
     *
     * <p>Keep the payload valid so the test targets listener processing and
     * retry recovery, rather than JSON deserialization failure.
     */
    private ProductEvent createFailingProductEvent() {
        return ProductEvent.builder()
               .schemaVersion(1)
               .eventId(UUID.randomUUID())
               .productId(POISON_PRODUCT_ID)
               .sku(POISON_SKU)
               .name("Faulty Product")
               .description("Triggers simulated processing failure")
               .price(BigDecimal.TEN)
               .stockQuantity(1)
               .eventType(ProductEvent.EventType.CREATED)
               .timestamp(LocalDateTime.now())
               .build();
    }

}
