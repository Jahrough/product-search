package dev.jahlopez.productsearch.integration;

import com.redis.testcontainers.RedisContainer;
import dev.jahlopez.productsearch.event.ProductEvent;
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
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class KafkaDlqIntegrationTest {

    private static final String MAIN_TOPIC = "product-events";
    private static final String DLT_TOPIC = MAIN_TOPIC + ".DLT";
    private static final String POISON_SKU = "POISON-PILL-SKU";
    private static final String POISON_PRODUCT_ID = "prod-poison-01";

    private static final Duration CONSUMER_POLL_TIMEOUT =
            Duration.ofMillis(200);
    private static final Duration DLT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DLT_POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration ASSIGNMENT_TIMEOUT =
            Duration.ofSeconds(10);

    @Container
    static final RedisContainer redis = new RedisContainer(
            DockerImageName.parse("redis:7-alpine")
    );

    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(
                    "confluentinc/cp-kafka:7.5.0"
            );

    @Container
    static final OpensearchContainer<?> opensearch =
            new OpensearchContainer<>(
                    DockerImageName.parse(
                            "opensearchproject/opensearch:2.11.0"
                    )
            );

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:15-alpine")
    );

    @Autowired
    private KafkaTemplate<String, ProductEvent> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add(
                "spring.data.redis.port",
                () -> redis.getMappedPort(6379)
        );

        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add(
                "spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers
        );
        registry.add(
                "spring.kafka.producer.key-serializer",
                () -> StringSerializer.class.getName()
        );
        registry.add(
                "spring.kafka.producer.value-serializer",
                () -> JsonSerializer.class.getName()
        );
        registry.add(
                "spring.kafka.consumer.auto-offset-reset",
                () -> "earliest"
        );

        registry.add(
                "app.opensearch.uris",
                opensearch::getHttpHostAddress
        );
    }

    @BeforeEach
    void waitForApplicationListeners() {
        for (MessageListenerContainer container
                : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    @Test
    @DisplayName(
            "Routes a poison-pill event to the DLT after retries are exhausted"
    )
    void shouldRoutePoisonPillToDlq() throws Exception {
        ProductEvent poisonPillEvent = createPoisonPillEvent();

        try (KafkaConsumer<String, String> dltConsumer =
                     createDltConsumer()) {

            subscribeAndAwaitAssignment(dltConsumer);

            kafkaTemplate.send(
                    MAIN_TOPIC,
                    poisonPillEvent.sku(),
                    poisonPillEvent
            ).get(10, TimeUnit.SECONDS);

            await()
                    .atMost(DLT_TIMEOUT)
                    .pollInterval(CONSUMER_POLL_TIMEOUT)
                    .untilAsserted(() ->
                            assertPoisonPillInDlt(
                                    dltConsumer,
                                    poisonPillEvent
                            )
                    );
        }
    }

    private KafkaConsumer<String, String> createDltConsumer() {
        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()
        );
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlq-test-" + UUID.randomUUID()
        );
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );
        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        return new KafkaConsumer<>(properties);
    }

    private void subscribeAndAwaitAssignment(
            KafkaConsumer<String, String> consumer) {

        consumer.subscribe(List.of(DLT_TOPIC));

        await()
                .atMost(ASSIGNMENT_TIMEOUT)
                .pollInterval(CONSUMER_POLL_TIMEOUT)
                .until(() -> {
                    consumer.poll(CONSUMER_POLL_TIMEOUT);
                    return !consumer.assignment().isEmpty();
                });
    }

    private ProductEvent createPoisonPillEvent() {
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

    private void assertPoisonPillInDlt(
            KafkaConsumer<String, String> consumer,
            ProductEvent expectedEvent) {

        ConsumerRecords<String, String> records =
                consumer.poll(DLT_POLL_TIMEOUT);

        assertThat(records)
                .anySatisfy(record -> {
                    assertThat(record.key())
                            .isEqualTo(expectedEvent.sku());

                    assertThat(record.value())
                            .contains(expectedEvent.productId())
                            .contains(expectedEvent.sku());
                });
    }
}