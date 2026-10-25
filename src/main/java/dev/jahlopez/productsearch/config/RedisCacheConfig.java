package dev.jahlopez.productsearch.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.time.Duration;
import java.util.Map;

/**
 * Configures Redis-backed caching for the product-search microservice.
 *
 * <p>This configuration enables Spring's cache abstraction and defines the
 * serialization, expiration, and cache-manager policies used by the service.
 * Application services can use annotations such as {@code @Cacheable},
 * {@code @CachePut}, and {@code @CacheEvict} to interact with these caches.</p>
 *
 * <p>Redis is a performance optimization, not the source of truth. Cached
 * entries must be safe to expire, invalidate, or rebuild from the underlying
 * data source.</p>
 *
 * <p>Cache names declared here must remain consistent with the names used by
 * application-level cache annotations. Changes to serialized value formats
 * should be coordinated with cache invalidation or a cache-key namespace
 * change to avoid incompatibility with existing entries.</p>
 *
 * <p>Redis should be accessible only to trusted application components.
 * Serialization restrictions are defense in depth and do not replace
 * network isolation, authentication, or authorization.</p>
 */
@Configuration(proxyBeanMethods = false)
@EnableCaching
public class RedisCacheConfig {

    /**
     * Default expiration for caches without a cache-specific policy.
     *
     * <p>Expiration limits how long an entry can remain in Redis without
     * being refreshed. It does not guarantee immediate consistency when
     * the underlying search index or product data changes.</p>
     */
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(60);

    /**
     * Expiration for individual product lookup results.
     *
     * <p>A shared duration keeps product-ID and SKU lookups consistent.
     * Adjust this value based on data freshness requirements and cache hit
     * rates.</p>
     */
    private static final Duration PRODUCT_LOOKUP_TTL = Duration.ofMinutes(30);

    /**
     * Expiration for product list and search results.
     *
     * <p>Search results may become stale as indexed products change, so
     * they use a shorter lifetime than individual product lookups.</p>
     */
    private static final Duration PRODUCT_LIST_TTL = Duration.ofMinutes(15);

    /** Cache name for product lookups by product ID. */
    public static final String PRODUCTS_BY_ID_CACHE = "productsById";

    /** Cache name for product lookups by SKU. */
    public static final String PRODUCTS_BY_SKU_CACHE = "productsBySku";

    /** Cache name for product list and search results. */
    public static final String PRODUCTS_LIST_CACHE = "productsList";

    /**
     * Per-cache expiration policies.
     *
     * <p>{@link Map#of(Object, Object, Object, Object, Object, Object)} creates
     * an immutable map, preventing runtime mutation of the cache policy.</p>
     *
     * <p>Caches not listed here inherit {@link #DEFAULT_TTL} from the shared
     * {@link RedisCacheConfiguration}.</p>
     */
    private static final Map<String, Duration> CACHE_TTLS = Map.of(
            PRODUCTS_BY_ID_CACHE, PRODUCT_LOOKUP_TTL,
            PRODUCTS_BY_SKU_CACHE, PRODUCT_LOOKUP_TTL,
            PRODUCTS_LIST_CACHE, PRODUCT_LIST_TTL
    );

    /**
     * Creates the shared Redis cache configuration.
     *
     * <p>Cache keys use string serialization for readability and predictable
     * key representation. Cache values use JSON serialization so values are
     * not stored as Java-native serialized objects.</p>
     *
     * <p>Polymorphic deserialization is restricted to the application's
     * product-search package and Java collection types. Only enable the
     * minimum set of types required by the actual cached values. Never treat
     * this allowlist as a substitute for trusting and securing the Redis
     * data source.</p>
     *
     * @return the default Redis cache configuration
     */
    @Bean
    public RedisCacheConfiguration cacheConfiguration() {
        // Limit polymorphic deserialization to application-owned types and
        // collection implementations needed by cached values.
        BasicPolymorphicTypeValidator typeValidator =
                BasicPolymorphicTypeValidator.builder()
                        .allowIfSubType("dev.jahlopez.productsearch.")
                        .allowIfSubType("java.util.")
                        .build();

        // Use JSON for cache values to avoid Java native object serialization.
        // Keep serializer settings compatible across service deployments.
        GenericJacksonJsonRedisSerializer jsonSerializer =
                GenericJacksonJsonRedisSerializer.builder()
                        .enableDefaultTyping(typeValidator)
                        .build();

        return RedisCacheConfiguration.defaultCacheConfig()
                // Apply a finite default lifetime to avoid indefinite staleness.
                .entryTtl(DEFAULT_TTL)

                // Null values are not cached; absent products can be queried
                // again. Consider a deliberate negative-cache policy if repeated
                // misses become expensive.
                .disableCachingNullValues()

                // Use readable string keys instead of binary key serialization.
                .serializeKeysWith(
                        RedisSerializationContext.SerializationPair
                                .fromSerializer(new StringRedisSerializer())
                )

                // Store cache values as JSON using the configured validator.
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair
                                .fromSerializer(jsonSerializer)
                );
    }

    /**
     * Creates the Redis-backed cache manager and applies per-cache expiration.
     *
     * <p>The manager uses the shared serialization configuration and default
     * TTL, then overrides the TTL for the named product caches. Declaring this
     * bean explicitly ensures that caching has a {@link CacheManager} in
     * contexts where Spring Boot's cache auto-configuration is not active.</p>
     *
     * <p>TTL is a freshness and storage policy, not an invalidation mechanism.
     * If product updates must become visible quickly, use explicit eviction
     * or event-driven invalidation in addition to expiration.</p>
     *
     * @param connectionFactory the Redis connection factory managed by Spring
     * @param cacheConfiguration the shared key, value, and default TTL settings
     * @return the configured Redis cache manager
     */
    @Bean
    public CacheManager cacheManager(
            RedisConnectionFactory connectionFactory,
            RedisCacheConfiguration cacheConfiguration
    ) {
        // Start with common serialization and expiration behavior.
        RedisCacheManager.RedisCacheManagerBuilder builder =
                RedisCacheManager.builder(connectionFactory)
                        .cacheDefaults(cacheConfiguration);

        // Override the default TTL for caches with explicit policies.
        CACHE_TTLS.forEach((cacheName, ttl) ->
                builder.withCacheConfiguration(
                        cacheName,
                        cacheConfiguration.entryTtl(ttl)
                )
        );

        // Build the manager after all cache policies have been registered.
        return builder.build();
    }
}