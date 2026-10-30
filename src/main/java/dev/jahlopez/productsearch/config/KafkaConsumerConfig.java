
package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.exception.InvalidProductEventException;
import dev.jahlopez.productsearch.exception.ProductEventConflictException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Configures product-event consumption and failed-message recovery.
 *
 * <p>Retryable listener failures use a bounded exponential backoff: 1s initial
 * interval, x2 multiplier, 30s maximum interval, 120s maximum elapsed time and a
 * 500ms jitter base (nominal schedule 1, 2, 4, 8, 16, 30, 30, 30s). Spring scales
 * the jitter by {@code interval / initialInterval}, so the applied jitter grows with
 * the interval (about +-8s at the 16s step) and is clamped to [initialInterval,
 * maxInterval]. The elapsed-time check runs before each attempt, so total retry time
 * is below 120s plus one interval (under 150s). When the budget is exhausted the
 * record is routed to {@code <original-topic>.DLT}. Retries block the partition.
 *
 * <p>{@code ProductEventConflictException} and {@code InvalidProductEventException}
 * (validation / unsupported event type) cannot succeed on retry and are sent
 * directly to the DLT. Every other exception, including an unrelated
 * {@code IllegalArgumentException} and infrastructure failures from PostgreSQL or
 * OpenSearch, remains retryable.
 *
 * <p>Ordering assumption: a single consumer per partition. The version guard in
 * the listener is check-then-write and does not protect against concurrent writers.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class KafkaConsumerConfig {

    static final long INITIAL_INTERVAL_MS = 1_000L;
    static final double MULTIPLIER = 2.0;
    static final long MAX_INTERVAL_MS = 30_000L;
    static final long MAX_ELAPSED_TIME_MS = 120_000L;
    static final long JITTER_MS = 500L;
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

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new ProductEventJsonDeserializer())
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, ProductEvent> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            BackOff productEventRetryBackOff
    ) {
        var factory =
                new ConcurrentKafkaListenerContainerFactory<String, ProductEvent>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(createErrorHandler(kafkaTemplate, productEventRetryBackOff));

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
    deadLetterKafkaListenerContainerFactory(Environment environment) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getRequiredProperty("spring.kafka.bootstrap-servers")
        );
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.auto-offset-reset",
                        "earliest"
                )
        );

        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                new ByteArrayDeserializer()
        ));
        return factory;
    }

    DefaultErrorHandler createErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        return createErrorHandler(kafkaTemplate, retryBackOff());
    }

    DefaultErrorHandler createErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            BackOff backOff
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + DEAD_LETTER_TOPIC_SUFFIX,
                        record.partition()
                )
        );

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                ProductEventConflictException.class,
                InvalidProductEventException.class
        );
        return errorHandler;
    }

    @Bean
    public BackOff productEventRetryBackOff() {
        return retryBackOff();
    }

    static ExponentialBackOff retryBackOff() {
        var backOff = new ExponentialBackOff(
                INITIAL_INTERVAL_MS,
                MULTIPLIER
        );
        backOff.setMaxInterval(MAX_INTERVAL_MS);
        backOff.setMaxElapsedTime(MAX_ELAPSED_TIME_MS);
        backOff.setJitter(JITTER_MS);
        return backOff;
    }
}
