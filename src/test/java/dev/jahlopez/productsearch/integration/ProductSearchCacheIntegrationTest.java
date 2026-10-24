package dev.jahlopez.productsearch.integration;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test suite for Redis caching and cache eviction behaviors within the
 * {@link ProductSearchService}.
 * <p>
 * Uses Testcontainers to spin up isolated Redis, OpenSearch, and PostgreSQL instances,
 * verifying that product lookups are properly cached and evicted upon deletion.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class ProductSearchCacheIntegrationTest {

    private static final String REDIS_IMAGE = "redis:7.2-alpine";
    private static final String OPENSEARCH_IMAGE =
            "opensearchproject/opensearch:2.11.0";
    private static final String POSTGRES_IMAGE = "postgres:15-alpine";

    private static final int REDIS_PORT = 6379;
    private static final int OPENSEARCH_PORT = 9200;

    private static final String CACHE_NAME = "products";
    private static final String PRODUCT_ID = "cache-prod-1";
    private static final String PRODUCT_SKU = "SKU-CACHE-01";

    // Must match ProductSearchService's @Cacheable ID key.
    private static final String PRODUCT_ID_CACHE_KEY = "id:" + PRODUCT_ID;

    /**
     * Redis Testcontainer for caching layer validation.
     */
    @Container
    private static final GenericContainer<?> REDIS_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                    .withExposedPorts(REDIS_PORT)
                    .waitingFor(
                            Wait.forListeningPort()
                                    .withStartupTimeout(Duration.ofMinutes(1))
                    );

    /**
     * OpenSearch Testcontainer for search query validation.
     */
    @Container
    private static final GenericContainer<?> OPENSEARCH_CONTAINER =
            new GenericContainer<>(
                    DockerImageName.parse(OPENSEARCH_IMAGE)
            )
                    .withEnv("discovery.type", "single-node")
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
                    .withEnv(
                            "OPENSEARCH_JAVA_OPTS",
                            "-Xms512m -Xmx512m"
                    )
                    .withExposedPorts(OPENSEARCH_PORT)
                    .waitingFor(
                            Wait.forHttp("/")
                                    .forPort(OPENSEARCH_PORT)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(2))
                    );

    /**
     * PostgreSQL Testcontainer for Flyway migrations and JPA repository initialization.
     */
    @Container
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE));

    /**
     * Dynamically injects container properties into the Spring Environment.
     */
    @DynamicPropertySource
    static void configureContainerProperties(
            DynamicPropertyRegistry registry
    ) {
        registry.add(
                "spring.data.redis.host",
                REDIS_CONTAINER::getHost
        );
        registry.add(
                "spring.data.redis.port",
                REDIS_CONTAINER::getFirstMappedPort
        );
        registry.add(
                "spring.datasource.url",
                POSTGRES_CONTAINER::getJdbcUrl
        );
        registry.add(
                "spring.datasource.username",
                POSTGRES_CONTAINER::getUsername
        );
        registry.add(
                "spring.datasource.password",
                POSTGRES_CONTAINER::getPassword
        );
        registry.add(
                "spring.datasource.driver-class-name",
                POSTGRES_CONTAINER::getDriverClassName
        );
        registry.add(
                "app.opensearch.uris",
                () -> "http://"
                        + OPENSEARCH_CONTAINER.getHost()
                        + ":"
                        + OPENSEARCH_CONTAINER.getMappedPort(OPENSEARCH_PORT)
        );
    }

    @Autowired
    private ProductSearchService productSearchService;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    private ProductDocument sampleDocument;

    @BeforeEach
    void setUp() {
        flushRedis();
        clearCaches();
        sampleDocument = createSampleProduct();
    }

    /**
     * Verifies that product lookups are cached on the first call and retrieved directly
     * from the cache on subsequent calls.
     */
    @Test
    @DisplayName("Should cache product lookups and return the cached result")
    void shouldCacheProductLookup() {
        // Arrange: Index sample product
        productSearchService.indexProduct(sampleDocument);

        // Act: First lookup should populate the Redis cache.
        ProductResponseDto firstResponse =
                productSearchService.getProductById(PRODUCT_ID);

        // Assert: Verify response and cache population
        assertThat(firstResponse).isNotNull();
        assertProductIsCached();

        // Act: Second lookup should return the cached value.
        ProductResponseDto secondResponse =
                productSearchService.getProductById(PRODUCT_ID);

        // Assert: Verify cached response parity
        assertThat(secondResponse).isNotNull();
        assertThat(secondResponse.getId())
                .isEqualTo(firstResponse.getId());
        assertThat(secondResponse.getSku())
                .isEqualTo(firstResponse.getSku());
    }

    /**
     * Verifies that product cache entries are properly evicted when a product index is deleted.
     */
    @Test
    @DisplayName("Should evict the product cache entry when its index is deleted")
    void shouldEvictCacheWhenProductIsDeleted() {
        // Arrange: Index product and populate cache
        productSearchService.indexProduct(sampleDocument);
        productSearchService.getProductById(PRODUCT_ID);

        assertProductIsCached();

        // Act: Delete product index (triggers @CacheEvict)
        productSearchService.deleteProductIndex(PRODUCT_ID);

        // Assert: Verify cache entry has been removed
        assertProductIsNotCached();
    }

    /**
     * Clears all configured application caches.
     */
    private void clearCaches() {
        for (String cacheName : cacheManager.getCacheNames()) {
            Cache cache = cacheManager.getCache(cacheName);
            if (cache != null) {
                cache.clear();
            }
        }
    }

    /**
     * Flushes the Redis keyspace to ensure test isolation.
     */
    private void flushRedis() {
        try (var connection = redisConnectionFactory.getConnection()) {
            connection.serverCommands().flushAll();
        }
    }

    /**
     * Creates a sample product document for caching tests.
     */
    private ProductDocument createSampleProduct() {
        return ProductDocument.builder()
                .id(PRODUCT_ID)
                .sku(PRODUCT_SKU)
                .name("Caching Test Mechanical Keyboard")
                .description("Redis cached description")
                .price(new BigDecimal("149.99"))
                .stockQuantity(25)
                .version(1L)
                .build();
    }

    /**
     * Asserts that the product exists in the Redis cache.
     */
    private void assertProductIsCached() {
        assertThat(getProductsCache().get(PRODUCT_ID_CACHE_KEY))
                .as("Product cache key '%s' should exist",
                        PRODUCT_ID_CACHE_KEY)
                .isNotNull();
    }

    /**
     * Asserts that the product has been evicted from the Redis cache.
     */
    private void assertProductIsNotCached() {
        assertThat(getProductsCache().get(PRODUCT_ID_CACHE_KEY))
                .as("Product cache key '%s' should have been evicted",
                        PRODUCT_ID_CACHE_KEY)
                .isNull();
    }

    /**
     * Retrieves the products cache instance from the cache manager.
     */
    private Cache getProductsCache() {
        return Objects.requireNonNull(
                cacheManager.getCache(CACHE_NAME),
                () -> "Cache '%s' is not configured".formatted(CACHE_NAME)
        );
    }
}