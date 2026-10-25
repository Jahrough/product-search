
package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.event.ProductEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configures product-event consumption and failed-message recovery.
 *
 * <p>Listener failures are retried three times with a one-second
 * interval. After retries are exhausted, the record is routed to
 * {@code <original-topic>.DLT}.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class KafkaConsumerConfig {

    private static final long RETRY_INTERVAL_MS = 1_000L;
    private static final long MAX_RETRIES = 3L;
    private static final String DEAD_LETTER_TOPIC_SUFFIX = ".DLT";

    @Bean
    public ConsumerFactory<String, ProductEvent> productEventConsumerFactory(
            Environment environment
    ) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getRequiredProperty("spring.kafka.bootstrap-servers")
        );
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.group-id",
                        "product-search"
                )
        );
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.auto-offset-reset",
                        "earliest"
                )
        );

        // Deserialize event payloads to ProductEvent without relying on
        // producer-supplied Java type headers.
        JacksonJsonDeserializer<ProductEvent> valueDeserializer = new JacksonJsonDeserializer<>(ProductEvent.class);
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                valueDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, ProductEvent> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        var factory =
                new ConcurrentKafkaListenerContainerFactory<String, ProductEvent>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(createErrorHandler(kafkaTemplate));

        return factory;
    }

    private DefaultErrorHandler createErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + DEAD_LETTER_TOPIC_SUFFIX,
                        record.partition()
                )
        );

        return new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES)
        );
    }
}
