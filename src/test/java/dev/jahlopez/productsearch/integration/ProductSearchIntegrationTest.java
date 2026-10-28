package dev.jahlopez.productsearch.integration;

import com.redis.testcontainers.RedisContainer;
import dev.jahlopez.productsearch.config.KafkaTestProducerConfig;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.exception.ResourceNotFoundException;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Kafka-driven product indexing and retrieval.
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(KafkaTestProducerConfig.class)
class ProductSearchIntegrationTest {

    private static final String MAIN_TOPIC = "product-events";
    private static final String INDEX_NAME = "products";

    private static final Long TEST_PRODUCT_ID = 12345L;
    private static final String TEST_SKU = "SRCH-HUB-01";
    private static final String TEST_PRODUCT_NAME =
            "Wireless Mechanical Gaming Keyboard";
    private static final String TEST_PRODUCT_DESC =
            "RGB backlit mechanical keyboard with tactile brown switches.";
    private static final BigDecimal TEST_PRICE = new BigDecimal("129.99");
    private static final int TEST_STOCK = 50;

    private static final Duration INDEXING_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration KAFKA_SEND_TIMEOUT = Duration.ofSeconds(10);

    @Container
    static final RedisContainer redis =
            new RedisContainer(DockerImageName.parse("redis:7-alpine"));

    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
            );

    @Container
    static final OpensearchContainer<?> opensearch =
            new OpensearchContainer<>(
                    DockerImageName.parse("opensearchproject/opensearch:2.11.0")
            )
                    .withEnv("discovery.type", "single-node")
                    .withEnv("DISABLE_SECURITY_PLUGIN", "true");

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("postgres:15-alpine")
            );

    @Autowired
    private ProductSearchService productSearchService;

    @Autowired
    private OpenSearchClient openSearchClient;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    /**
     * Routes application connections to isolated Testcontainers services and configures
     * DB schema creation and Kafka deserialization options for tests.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add(
                "spring.datasource.driver-class-name",
                postgres::getDriverClassName
        );
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create");

        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add(
                "spring.kafka.producer.key-serializer",
                () -> StringSerializer.class.getName()
        );
        registry.add(
                "spring.kafka.producer.value-serializer",
                () -> JacksonJsonSerializer.class.getName()
        );
        registry.add(
                "spring.kafka.consumer.auto-offset-reset",
                () -> "earliest"
        );
        registry.add("app.opensearch.uris", opensearch::getHttpHostAddress);

        // Prevent ResourceNotFoundException and IllegalStateException from tripping the circuit breaker during polling
        registry.add(
                "resilience4j.circuitbreaker.instances.openSearchService.ignore-exceptions[0]",
                () -> ResourceNotFoundException.class.getName()
        );
        registry.add(
                "resilience4j.circuitbreaker.instances.openSearchService.ignore-exceptions[1]",
                () -> IllegalStateException.class.getName()
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        recreateProductIndex();
        waitForKafkaListeners();
    }

    @Test
    @DisplayName(
            "Should consume a product-created event, index it in OpenSearch, "
                    + "and retrieve it through the service"
    )
    void shouldConsumeEventAndSearchProduct() throws Exception {
        ProductEvent event = createProductCreatedEvent();

        // Wait for Kafka to acknowledge publication before asserting processing.
        kafkaTemplate.send(MAIN_TOPIC, TEST_SKU, event)
                .get(KAFKA_SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        await().alias("product event should be indexed and searchable")
                .atMost(INDEXING_TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> {
                    // Explicitly refresh OpenSearch index so documents become searchable immediately
                    openSearchClient.indices()
                            .refresh(request -> request.index(INDEX_NAME));

                    try {
                        ProductResponseDto fetched =
                                productSearchService.getProductById(TEST_PRODUCT_ID.toString());

                        assertThat(fetched).isNotNull();
                        assertThat(fetched.getId()).isEqualTo(TEST_PRODUCT_ID.toString());
                        assertThat(fetched.getSku()).isEqualTo(TEST_SKU);
                        assertThat(fetched.getName()).isEqualTo(TEST_PRODUCT_NAME);
                    } catch (ResourceNotFoundException e) {
                        // Throw AssertionError so Awaitility continues polling until timeout
                        throw new AssertionError("Product not yet indexed/found", e);
                    }
                });
    }

    /**
     * Recreates the search index to isolate the test from prior indexed data.
     */
    private void recreateProductIndex() throws Exception {
        boolean indexExists = openSearchClient.indices()
                .exists(ExistsRequest.of(request -> request.index(INDEX_NAME)))
                .value();

        if (indexExists) {
            openSearchClient.indices().delete(
                    DeleteIndexRequest.of(request -> request.index(INDEX_NAME))
            );
        }

        openSearchClient.indices().create(
                CreateIndexRequest.of(request -> request.index(INDEX_NAME))
        );
    }

    /**
     * Waits for Kafka listener containers to receive their partition assignments.
     */
    private void waitForKafkaListeners() {
        for (MessageListenerContainer container
                : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    private ProductEvent createProductCreatedEvent() {
        return ProductEvent.builder()
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(1L)
                .productId(TEST_PRODUCT_ID)
                .sku(TEST_SKU)
                .name(TEST_PRODUCT_NAME)
                .description(TEST_PRODUCT_DESC)
                .price(TEST_PRICE)
                .stockQuantity(TEST_STOCK)
                .eventType(ProductEvent.EventType.CREATED)
                .timestamp(LocalDateTime.now())
                .build();
    }
}