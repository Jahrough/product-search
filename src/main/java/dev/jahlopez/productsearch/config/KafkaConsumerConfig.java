package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.event.ProductEvent;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configures Kafka listeners and failure recovery for product events.
 *
 * <p>After exhausting retries, failed events are published to the
 * corresponding dead-letter topic ({@code <original-topic>.DLT}).
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private static final long RETRY_INTERVAL_MS = 1_000L;
    private static final long MAX_RETRIES = 3L;
    private static final String DEAD_LETTER_TOPIC_SUFFIX = ".DLT";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, ProductEvent> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        // Create the listener factory and configure its consumer.
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ProductEvent>();
        factory.setConsumerFactory(consumerFactory);

        // Apply retry and dead-letter recovery behavior to listener failures.
        factory.setCommonErrorHandler(createErrorHandler(kafkaTemplate));

        return factory;
    }

    private DefaultErrorHandler createErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Publish messages to the dead-letter topic after retries are exhausted.
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> resolveDeadLetterTopic(record));

        // Configure a fixed one-second delay between retry attempts.
        var backOff = new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES);

        // Retry failed deliveries before invoking the dead-letter recoverer.
        return new DefaultErrorHandler(recoverer, backOff);
    }

    private TopicPartition resolveDeadLetterTopic(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record) {
        // Preserve the original topic name and append the dead-letter suffix.
        String deadLetterTopic = record.topic() + DEAD_LETTER_TOPIC_SUFFIX;

        // Preserve the original partition when routing the failed message.
        return new TopicPartition(deadLetterTopic, record.partition());
    }
}