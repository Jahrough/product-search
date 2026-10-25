package dev.jahlopez.productsearch.integration;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test suite for Redis caching and cache eviction behaviors within the
 * {@link ProductSearchService}.
 * <p>
 * Uses Testcontainers to spin up isolated Redis, OpenSearch, and PostgreSQL instances,
 * verifying that product lookups are properly cached and evicted upon deletion.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@TestPropertySource(properties = "spring.main.allow-bean-definition-overriding=true")
class ProductSearchCacheIntegrationTest {

    private static final String REDIS_IMAGE = "redis:7.2-alpine";
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.0";
    private static final String POSTGRES_IMAGE = "postgres:15-alpine";

    private static final int REDIS_PORT = 6379;
    private static final int OPENSEARCH_PORT = 9200;

    private static final String PRODUCT_ID = "cache-prod-1";
    private static final String PRODUCT_SKU = "SKU-CACHE-01";

    // Matches ProductSearchService's @Cacheable(key = "'id:' + #id") expression.
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
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
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

    /**
     * Test configuration override using Jackson 3 (tools.jackson) without polymorphic typing headers.
     */
    @TestConfiguration
    static class TestRedisConfiguration {

        @Bean
        public RedisCacheConfiguration cacheConfiguration() {
            ObjectMapper objectMapper = new ObjectMapper();

            RedisSerializer<Object> serializer = new RedisSerializer<>() {
                @Override
                public byte[] serialize(Object t) throws SerializationException {
                    if (t == null) {
                        return new byte[0];
                    }
                    try {
                        return objectMapper.writeValueAsBytes(t);
                    } catch (Exception e) {
                        throw new SerializationException("Could not write JSON: " + e.getMessage(), e);
                    }
                }

                @Override
                public Object deserialize(byte[] bytes) throws SerializationException {
                    if (bytes == null || bytes.length == 0) {
                        return null;
                    }
                    try {
                        // Deserialize directly into ProductResponseDto so Spring Cache doesn't throw a ClassCastException from LinkedHashMap
                        return objectMapper.readValue(bytes, ProductResponseDto.class);
                    } catch (Exception e) {
                        throw new SerializationException("Could not read JSON: " + e.getMessage(), e);
                    }
                }
            };

            return RedisCacheConfiguration.defaultCacheConfig()
                    .entryTtl(Duration.ofMinutes(60))
                    .serializeValuesWith(
                            RedisSerializationContext.SerializationPair.fromSerializer(serializer)
                    );
        }
    }

    @Autowired
    private ProductSearchService productSearchService;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

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
            var cache = cacheManager.getCache(cacheName);
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
        } catch (Exception e) {
            // Fallback or ignore if connection is already closed/uninitialized during setup
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
     * Asserts that keys matching the product cache key exist in Redis.
     */
    private void assertProductIsCached() {
        Set<String> keys = stringRedisTemplate.keys("*" + PRODUCT_ID_CACHE_KEY);
        assertThat(keys)
                .as("Redis should contain keys matching pattern '*%s'", PRODUCT_ID_CACHE_KEY)
                .isNotNull()
                .isNotEmpty();
    }

    /**
     * Asserts that the product cache key has been evicted from Redis.
     */
    private void assertProductIsNotCached() {
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    Set<String> keys = stringRedisTemplate.keys("*" + PRODUCT_ID_CACHE_KEY);
                    assertThat(keys)
                            .as("Redis cache keys matching '*%s' should have been evicted", PRODUCT_ID_CACHE_KEY)
                            .isNullOrEmpty();
                });
    }
}