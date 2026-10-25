package dev.jahlopez.productsearch.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RedisCacheConfig}.
 *
 * <p>Starts a disposable Redis container and verifies that the application's
 * Redis cache configuration can initialize and expose the expected caches.</p>
 *
 * <p>This test loads only the cache configuration and the Redis connection
 * factory. It intentionally avoids starting the entire application, including
 * unrelated database, Kafka, and search infrastructure.</p>
 */
@SpringJUnitConfig(RedisCacheConfigTest.TestConfig.class)
@Testcontainers
class RedisCacheConfigTest {

    private static final int REDIS_PORT = 6379;

    /**
     * Disposable Redis instance shared by the tests in this class.
     *
     * <p>Testcontainers maps the container port to an available host port,
     * avoiding assumptions that Redis is running locally on port 6379.</p>
     */
    @Container
    static final GenericContainer<?> REDIS_CONTAINER =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(REDIS_PORT);

    /**
     * Publishes the mapped Redis address for Spring's test environment.
     *
     * <p>These properties are useful to other Redis-aware components loaded
     * by the test context. The connection factory below explicitly uses the
     * same container address.</p>
     */
    @DynamicPropertySource
    static void configureRedisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
        registry.add("spring.data.redis.port", REDIS_CONTAINER::getFirstMappedPort);
    }

    /**
     * Minimal Spring context for testing Redis caching in isolation.
     */
    @Configuration(proxyBeanMethods = false)
    @Import(RedisCacheConfig.class)
    static class TestConfig {

        /**
         * Creates a Redis connection factory targeting the Testcontainers
         * Redis instance rather than a developer's local Redis installation.
         *
         * @return the configured Redis connection factory
         */
        @Bean
        RedisConnectionFactory redisConnectionFactory() {
            return new LettuceConnectionFactory(
                    REDIS_CONTAINER.getHost(),
                    REDIS_CONTAINER.getFirstMappedPort()
            );
        }
    }

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Test
    @DisplayName("Should initialize the Redis cache manager and connection factory")
    void shouldInitializeRedisCacheInfrastructure() {
        assertThat(cacheManager).isNotNull();
        assertThat(redisConnectionFactory).isNotNull();
    }

    @Test
    @DisplayName("Should expose all configured product caches")
    void shouldExposeExpectedProductCaches() {
        assertThat(cacheManager.getCache(RedisCacheConfig.PRODUCTS_BY_ID_CACHE))
                .as("cache for product ID lookups")
                .isNotNull();

        assertThat(cacheManager.getCache(RedisCacheConfig.PRODUCTS_BY_SKU_CACHE))
                .as("cache for SKU lookups")
                .isNotNull();

        assertThat(cacheManager.getCache(RedisCacheConfig.PRODUCTS_LIST_CACHE))
                .as("cache for product lists and search results")
                .isNotNull();
    }
}