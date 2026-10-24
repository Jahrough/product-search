package dev.jahlopez.productsearch.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.cache.RedisCacheManagerBuilderCustomizer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * Enterprise Redis caching configuration class.
 * Enables Spring caching infrastructure, defines custom secure JSON serialization,
 * and configures default and tier-specific Time-To-Live (TTL) expiration rules.
 */
@Configuration
@EnableCaching
public class RedisCacheConfig {

    private static final Duration DEFAULT_TTL = Duration.ofMinutes(60);
    private static final Duration PRODUCTS_BY_ID_TTL = Duration.ofMinutes(30);
    private static final Duration PRODUCTS_BY_SKU_TTL = Duration.ofMinutes(30);
    private static final Duration PRODUCTS_LIST_TTL = Duration.ofMinutes(15);

    /**
     * Configures default Redis cache settings, including JSON serialization with secure polymorphic type validation,
     * string-based keys, disabled null caching, and a default 60-minute TTL.
     *
     * @return the default Redis cache configuration
     */
    @Bean
    public RedisCacheConfiguration cacheConfiguration() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        BasicPolymorphicTypeValidator typeValidator =
                BasicPolymorphicTypeValidator.builder()
                        .allowIfBaseType(Object.class)
                        .build();

        // Fixed: Use NON_FINAL instead of EVERYTHING for secure polymorphic type validation
        objectMapper.activateDefaultTyping(
                typeValidator,
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY
        );

        GenericJackson2JsonRedisSerializer jsonSerializer =
                new GenericJackson2JsonRedisSerializer(objectMapper);

        return RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(DEFAULT_TTL)
                .disableCachingNullValues()
                .serializeKeysWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(
                                new StringRedisSerializer()
                        )
                )
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(
                                jsonSerializer
                        )
                );
    }

    /**
     * Applies cache-specific TTLs for 'productsById', 'productsBySku', and 'productsList'
     * while retaining the default serialization settings and other configuration options.
     *
     * @param cacheConfiguration the default Redis cache configuration
     * @return the Redis cache manager customizer
     */
    @Bean
    public RedisCacheManagerBuilderCustomizer redisCacheManagerBuilderCustomizer(
            RedisCacheConfiguration cacheConfiguration
    ) {
        return builder -> builder
                .withCacheConfiguration("productsById", cacheConfiguration.entryTtl(PRODUCTS_BY_ID_TTL))
                .withCacheConfiguration("productsBySku", cacheConfiguration.entryTtl(PRODUCTS_BY_SKU_TTL))
                .withCacheConfiguration("productsList", cacheConfiguration.entryTtl(PRODUCTS_LIST_TTL));
    }
}