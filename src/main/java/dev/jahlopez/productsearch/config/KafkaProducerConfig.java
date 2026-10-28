
package dev.jahlopez.productsearch.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Configures the Kafka producer used for application events
 * and dead-letter publishing.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> productEventProducerFactory(
            Environment environment
    ) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getRequiredProperty("spring.kafka.bootstrap-servers")
        );

        Map<Class<?>, Serializer<?>> valueSerializers = new LinkedHashMap<>();
        valueSerializers.put(byte[].class, new ByteArraySerializer());
        valueSerializers.put(Object.class, new JacksonJsonSerializer<>());

        return new DefaultKafkaProducerFactory<>(
                properties,
                new StringSerializer(),
                new DelegatingByTypeSerializer(valueSerializers, true)
        );
    }

    @Bean
    public KafkaTemplate<String, Object> productEventKafkaTemplate(
            ProducerFactory<String, Object> productEventProducerFactory
    ) {
        return new KafkaTemplate<>(productEventProducerFactory);
    }
}
