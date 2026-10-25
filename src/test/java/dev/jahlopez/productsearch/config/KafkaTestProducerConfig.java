package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.event.ProductEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Test configuration class providing Kafka producer beans specifically tailored
 * for integration testing environments.
 * <p>
 * This configuration overrides production beans to supply a test-specific
 * {@link KafkaTemplate} and {@link ProducerFactory} utilizing string serialization
 * for keys and JSON serialization for {@link ProductEvent} payloads.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestProducerConfig {

    /**
     * Creates a Kafka producer factory configured for integration tests with string keys
     * and JSON-serialized {@link ProductEvent} values.
     *
     * @param bootstrapServers the Kafka broker bootstrap servers address, injected from test properties
     * @return a configured {@link ProducerFactory} instance for string keys and product event values
     */
    @Bean
    public ProducerFactory<String, ProductEvent> productEventTestProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new DefaultKafkaProducerFactory<>(
                properties,
                new StringSerializer(),
                new JacksonJsonSerializer<>()
        );
    }

    /**
     * Defines the primary {@link KafkaTemplate} bean used to publish {@link ProductEvent}
     * messages during integration tests.
     *
     * @param productEventTestProducerFactory the producer factory bean supplying connection and serialization settings
     * @return a primary {@link KafkaTemplate} instance for test message publishing
     */
    @Bean
    @Primary
    public KafkaTemplate<String, ProductEvent> kafkaTemplate(
            ProducerFactory<String, ProductEvent> productEventTestProducerFactory
    ) {
        return new KafkaTemplate<>(productEventTestProducerFactory);
    }
}