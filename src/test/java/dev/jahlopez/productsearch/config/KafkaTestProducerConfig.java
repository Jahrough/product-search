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
 * Provides a typed Kafka producer for integration tests.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestProducerConfig {

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

    @Bean
    @Primary
    public KafkaTemplate<String, ProductEvent> kafkaTemplate(
            ProducerFactory<String, ProductEvent> productEventTestProducerFactory
    ) {
        return new KafkaTemplate<>(productEventTestProducerFactory);
    }
}