package dev.jahlopez.productsearch.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Test configuration class providing Kafka producer beans specifically tailored
 * for integration testing environments.
 * <p>
 * This configuration overrides production beans to supply a test-specific
 * {@link KafkaTemplate} and {@link ProducerFactory} utilizing string serialization
 * for keys, JSON event payloads, and raw byte-array dead-letter payloads.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestProducerConfig {

    /**
     * Creates a Kafka producer factory configured for integration tests with string keys
     * with JSON serialization for application events and byte-array
     * serialization for original failed payloads.
     *
     * @param bootstrapServers the Kafka broker bootstrap servers address, injected from test properties
     * @return a configured {@link ProducerFactory} instance for string keys and product event values
     */
    @Bean
    public ProducerFactory<String, Object> productEventTestProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Map<Class<?>, Serializer<?>> valueSerializers = new LinkedHashMap<>();
        valueSerializers.put(byte[].class, new ByteArraySerializer());
        valueSerializers.put(Object.class, new JacksonJsonSerializer<>());

        return new DefaultKafkaProducerFactory<>(
                properties,
                new StringSerializer(),
                new DelegatingByTypeSerializer(valueSerializers, true)
        );
    }

    /**
     * Defines the primary {@link KafkaTemplate} bean used to publish events and
     * dead-letter payloads during integration tests.
     *
     * @param productEventTestProducerFactory the producer factory bean supplying connection and serialization settings
     * @return a primary {@link KafkaTemplate} instance for test message publishing
     */
    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> productEventTestProducerFactory
    ) {
        return new KafkaTemplate<>(productEventTestProducerFactory);
    }
}