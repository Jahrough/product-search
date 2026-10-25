
package dev.jahlopez.productsearch.integration;

import com.redis.testcontainers.RedisContainer;
import dev.jahlopez.productsearch.config.KafkaTestProducerConfig;
import dev.jahlopez.productsearch.event.ProductEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Kafka dead-letter handling.
 *
 * <p>Starts Kafka, PostgreSQL, Redis, and OpenSearch containers and
 * exercises the application listener using the real Kafka broker.
 *
 * <p>The test verifies that an event which repeatedly fails during
 * listener processing is eventually published to the dead-letter topic.
 *
 * <p>Prerequisite: the application listener must deliberately reject
 * {@value #POISON_SKU}, or another deterministic failure must be configured
 * for this test. A valid {@link ProductEvent} does not fail automatically.
 */
@SpringBootTest
@Testcontainers
@Import(KafkaTestProducerConfig.class)
class KafkaDlqIntegrationTest {

    private static final String MAIN_TOPIC = "product-events";
    private static final String DLT_TOPIC = MAIN_TOPIC + ".DLT";

    private static final String POISON_SKU = "POISON-PILL-SKU";
    private static final String POISON_PRODUCT_ID = "prod-poison-01";

    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration DLT_POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration DLT_DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration LISTENER_ASSIGNMENT_TIMEOUT =
            Duration.ofSeconds(10);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    // Infrastructure shared by Spring's application context and this test.
    @Container
    static final RedisContainer redis =
            new RedisContainer(DockerImageName.parse("redis:7-alpine"));

    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.0");

    @Container
    static final OpensearchContainer<?> opensearch =
            new OpensearchContainer<>(
                    DockerImageName.parse(
                            "opensearchproject/opensearch:2.11.0"));

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("postgres:15-alpine"));

    @Autowired
    private KafkaTemplate<String, ProductEvent> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    /**
     * Overrides application infrastructure endpoints with container endpoints.
     *
     * <p>These properties allow the production application configuration
     * to run against isolated test infrastructure.
     */
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
        try (KafkaConsumer<String, String> dltConsumer = createDltConsumer()) {
            subscribeAndAwaitAssignment(dltConsumer);

            kafkaTemplate.send(MAIN_TOPIC, event.sku(), event)
                    .get(SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            // Kafka processing and retries are asynchronous.
            await()
                    .alias("event to arrive on " + DLT_TOPIC)
                    .atMost(DLT_DELIVERY_TIMEOUT)
                    .pollInterval(POLL_INTERVAL)
                    .untilAsserted(() -> assertEventInDeadLetterTopic(
                            dltConsumer, event));
        }
    }

    /**
     * Creates an independent consumer for inspecting the DLT.
     *
     * <p>A unique group ID avoids inheriting committed offsets from another
     * test run. String deserialization is appropriate for inspecting the
     * serialized JSON payload produced by the configured DLT publisher.
     */
    private KafkaConsumer<String, String> createDltConsumer() {
        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers());
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlt-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        return new KafkaConsumer<>(properties);
    }

    /**
     * Subscribes the DLT consumer and waits until Kafka assigns a partition.
     */
    private void subscribeAndAwaitAssignment(
            KafkaConsumer<String, String> consumer) {
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

    /**
     * Asserts that the DLT contains the expected event key and payload.
     *
     * <p>This checks identifying fields rather than the entire JSON string,
     * avoiding brittle assertions about JSON property order or formatting.
     */
    private void assertEventInDeadLetterTopic(
            KafkaConsumer<String, String> consumer,
            ProductEvent expectedEvent) {
        ConsumerRecords<String, String> records =
                consumer.poll(DLT_POLL_TIMEOUT);

        assertThat(records)
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(expectedEvent.sku());
                    assertThat(record.value())
                            .contains(expectedEvent.productId())
                            .contains(expectedEvent.sku());
                });
    }
}
