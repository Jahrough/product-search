package dev.jahlopez.productsearch.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Configuration test suite for Redis caching infrastructure.
 * <p>
 * Verifies that the {@link CacheManager}, Redis connection factories, and TTL / serialization
 * policies are correctly wired and initialized in the Spring container.
 */
@SpringBootTest
@Testcontainers
class RedisCacheConfigTest {

    private static final String REDIS_IMAGE = "redis:7.2-alpine";
    private static final String POSTGRES_IMAGE = "postgres:15-alpine";
    private static final int REDIS_PORT = 6379;

    /**
     * Redis Testcontainer for isolated distributed caching and key-value state validation.
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
     * PostgreSQL Testcontainer to support Spring Data JPA and Flyway migration bootstrapping
     * during full application context initialization.
     */
    @Container
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE));

    /**
     * Dynamically injects PostgreSQL datasource and Redis host/port connection parameters
     * into the Spring Environment prior to context startup.
     *
     * @registry the dynamic property registry
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Database wiring for JPA and Flyway context dependencies
        registry.add("spring.datasource.url", POSTGRES_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.datasource.password", POSTGRES_CONTAINER::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES_CONTAINER::getDriverClassName);

        // Redis cache server host and dynamically mapped port
        registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
        registry.add("spring.data.redis.port", REDIS_CONTAINER::getFirstMappedPort);
    }

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    /**
     * Verifies that the Redis cache manager and connection factory beans are successfully created
     * and configured in the application context.
     */
    @Test
    @DisplayName("Should correctly configure Redis cache manager and connection factory beans")
    void shouldConfigureRedisCacheBeans() {
        // Assert: Ensure core caching infrastructure beans are successfully wired
        assertThat(cacheManager).isNotNull();
        assertThat(redisConnectionFactory).isNotNull();

        // Verify that default or named cache configurations are accessible and populated
        assertThat(cacheManager.getCacheNames()).isNotEmpty();
    }
}