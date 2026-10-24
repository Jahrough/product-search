package dev.jahlopez.productsearch.integration;

import com.redis.testcontainers.RedisContainer;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test suite for the `product-search` microservice.
 * <p>
 * Validates the complete messaging and search lifecycle: producing a product event to Kafka,
 * consuming it via {@link dev.jahlopez.productsearch.listener.ProductEventConsumer}, persisting
 * the inbox deduplication record in PostgreSQL, indexing the document in OpenSearch, and verifying
 * retrieval via {@link ProductSearchService}.
 */
@SpringBootTest
@Testcontainers
class ProductSearchIntegrationTest {

    private static final String MAIN_TOPIC = "product-events";
    private static final String INDEX_NAME = "products";

    private static final String TEST_PRODUCT_ID = "prod-12345";
    private static final String TEST_SKU = "SRCH-HUB-01";
    private static final String TEST_PRODUCT_NAME =
            "Wireless Mechanical Gaming Keyboard";
    private static final String TEST_PRODUCT_DESC =
            "RGB backlit mechanical keyboard with tactile brown switches.";
    private static final BigDecimal TEST_PRICE = new BigDecimal("129.99");
    private static final int TEST_STOCK = 50;

    private static final Duration INDEXING_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    /**
     * Redis Testcontainer for distributed caching and state management.
     */
    @Container
    static final RedisContainer redis = new RedisContainer(
            DockerImageName.parse("redis:7-alpine")
    );

    /**
     * Confluent Kafka Testcontainer for event-driven messaging validation.
     */
    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.0");

    /**
     * OpenSearch Testcontainer configured as a single-node search engine cluster.
     */
    @Container
    static final OpensearchContainer<?> opensearch =
            new OpensearchContainer<>(
                    DockerImageName.parse(
                            "opensearchproject/opensearch:2.11.0"
                    )
            )
                    .withEnv("discovery.type", "single-node")
                    .withEnv("DISABLE_SECURITY_PLUGIN", "true");

    /**
     * PostgreSQL Testcontainer for Flyway migrations and consumer inbox persistence.
     */
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:15-alpine")
    );

    @Autowired
    private ProductSearchService productSearchService;

    @Autowired
    private OpenSearchClient openSearchClient;

    @Autowired
    private KafkaTemplate<String, ProductEvent> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    /**
     * Dynamically overrides application properties to point to the isolated Testcontainer instances.
     *
     * @registry the dynamic property registry
     */
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
        registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);

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

    /**
     * Prepares the test environment before each test execution by resetting the search index
     * and ensuring Kafka listener containers have successfully established topic partition assignments.
     *
     * @throws Exception if index setup or listener synchronization fails
     */
    @BeforeEach
    void setUp() throws Exception {
        recreateProductIndex();
        waitForKafkaListeners();
    }

    /**
     * Verifies that producing a `CREATED` product event to Kafka results in successful consumer processing,
     * OpenSearch indexing, and subsequent retrievability via the product search service.
     *
     * @throws Exception if message dispatch or assertion polling fails
     */
    @Test
    @DisplayName(
            "Should consume a product-created event, index it in OpenSearch, "
                    + "and retrieve it through the service"
    )
    void shouldConsumeEventAndSearchProduct() throws Exception {
        // Arrange: Build sample product creation event
        ProductEvent event = createProductCreatedEvent();

        // Act: Publish event to Kafka main topic
        kafkaTemplate.send(MAIN_TOPIC, TEST_SKU, event)
                .get(10, TimeUnit.SECONDS);

        // Assert: Await asynchronous consumer processing and verify document availability
        await()
                .alias("product event should be indexed and searchable")
                .atMost(INDEXING_TIMEOUT)
                .pollInterval(POLL_INTERVAL)
                .ignoreExceptionsInstanceOf(
                        ResourceNotFoundException.class
                )
                .untilAsserted(() -> assertProductIsSearchable(event));
    }

    /**
     * Recreates the OpenSearch products index to ensure test isolation.
     */
    private void recreateProductIndex() throws Exception {
        boolean indexExists = openSearchClient.indices()
                .exists(ExistsRequest.of(request ->
                        request.index(INDEX_NAME)))
                .value();

        if (indexExists) {
            openSearchClient.indices().delete(
                    DeleteIndexRequest.of(request ->
                            request.index(INDEX_NAME))
            );
        }

        openSearchClient.indices().create(
                CreateIndexRequest.of(request ->
                        request.index(INDEX_NAME))
        );
    }

    /**
     * Waits for Kafka listener containers to be assigned partitions before proceeding with test execution.
     */
    private void waitForKafkaListeners() {
        for (MessageListenerContainer container
                : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    /**
     * Creates a sample product creation event payload.
     */
    private ProductEvent createProductCreatedEvent() {
        return ProductEvent.builder()
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

    /**
     * Refreshes OpenSearch segments and verifies that the product can be retrieved
     * successfully by both ID and SKU.
     */
    private void assertProductIsSearchable(ProductEvent event)
            throws Exception {

        // Refresh makes already-indexed documents visible to search queries immediately.
        openSearchClient.indices().refresh(request ->
                request.index(INDEX_NAME));

        ProductResponseDto fetchedById =
                productSearchService.getProductById(event.productId());

        assertThat(fetchedById).isNotNull();
        assertThat(fetchedById.getId()).isEqualTo(event.productId());
        assertThat(fetchedById.getSku()).isEqualTo(event.sku());
        assertThat(fetchedById.getName()).isEqualTo(event.name());

        ProductResponseDto fetchedBySku =
                productSearchService.getProductBySku(event.sku());

        assertThat(fetchedBySku).isNotNull();
        assertThat(fetchedBySku.getId()).isEqualTo(event.productId());
        assertThat(fetchedBySku.getName()).isEqualTo(event.name());
    }
}