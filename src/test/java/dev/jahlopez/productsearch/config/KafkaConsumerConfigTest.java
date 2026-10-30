package dev.jahlopez.productsearch.config;

import dev.jahlopez.productsearch.exception.InvalidProductEventException;
import dev.jahlopez.productsearch.exception.ProductEventConflictException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    @DisplayName("Should configure bounded exponential backoff")
    void shouldConfigureBoundedExponentialBackOff() {
        var backOff = KafkaConsumerConfig.retryBackOff();

        assertThat(backOff.getInitialInterval()).isEqualTo(1_000L);
        assertThat(backOff.getMultiplier()).isEqualTo(2.0);
        assertThat(backOff.getMaxInterval()).isEqualTo(30_000L);
        assertThat(backOff.getMaxElapsedTime()).isEqualTo(120_000L);
        assertThat(backOff.getJitter()).isEqualTo(500L);
    }

    @Test
    @DisplayName("Should produce a finite retry schedule within the elapsed-time budget")
    void shouldProduceFiniteSchedule() {
        BackOffExecution execution = KafkaConsumerConfig.retryBackOff().start();
        List<Long> intervals = new ArrayList<>();
        long next;
        while ((next = execution.nextBackOff()) != BackOffExecution.STOP) {
            intervals.add(next);
            assertThat(intervals.size()).isLessThan(50);
        }

        System.out.println("Retry schedule (ms): " + intervals
                + " total=" + intervals.stream().mapToLong(Long::longValue).sum());
        assertThat(intervals).hasSizeBetween(5, 12);
        assertThat(intervals).allSatisfy(interval -> assertThat(interval).isLessThanOrEqualTo(30_000L));
        // The elapsed-time check runs before each attempt, so the total can overshoot by one interval.
        assertThat(intervals.stream().mapToLong(Long::longValue).sum())
                .isLessThan(120_000L + 30_000L);
    }

    @Test
    @DisplayName("Should send an invalid product event straight to the DLT without retrying")
    void shouldNotRetryInvalidProductEvent() {
        assertRecoveredImmediately(new InvalidProductEventException("invalid"));
    }

    @Test
    @DisplayName("Should keep an invalid product event non-retryable when wrapped")
    void shouldNotRetryWrappedInvalidProductEvent() {
        var invalid = new InvalidProductEventException("invalid");
        assertRecoveredImmediately(new ListenerExecutionFailedException("listener failed", invalid));
        assertRecoveredImmediately(new RuntimeException("outer", new IllegalStateException("middle", invalid)));
    }

    @Test
    @DisplayName("Should keep event conflicts non-retryable, directly and wrapped")
    void shouldNotRetryConflict() {
        var conflict = new ProductEventConflictException("conflict");
        assertRecoveredImmediately(conflict);
        assertRecoveredImmediately(new ListenerExecutionFailedException("listener failed", conflict));
    }

    @Test
    @DisplayName("Should retry an unrelated IllegalArgumentException before recovering")
    void shouldRetryUnrelatedIllegalArgumentException() {
        assertRetriedBeforeRecovery(new IllegalArgumentException("unrelated"));
    }

    @Test
    @DisplayName("Should retry an unrelated IllegalArgumentException wrapped by a framework exception")
    void shouldRetryWrappedUnrelatedIllegalArgumentException() {
        var unrelated = new IllegalArgumentException("unrelated");
        assertRetriedBeforeRecovery(new InvalidDataAccessApiUsageException("jpa", unrelated));
        assertRetriedBeforeRecovery(new ListenerExecutionFailedException("listener failed", unrelated));
    }

    @Test
    @DisplayName("Should keep infrastructure failures retryable")
    void shouldRetryInfrastructureFailures() {
        assertRetriedBeforeRecovery(new IllegalStateException("OpenSearch call failed", new IOException("io")));
        assertRetriedBeforeRecovery(new OpenSearchException(ErrorResponse.of(r -> r
                .status(503)
                .error(e -> e.type("unavailable").reason("unavailable")))));
        assertRetriedBeforeRecovery(new DataAccessResourceFailureException("PostgreSQL unavailable"));
    }

    private void assertRecoveredImmediately(Exception failure) {
        KafkaTemplate<String, Object> template = successfulTemplate();
        DefaultErrorHandler handler = config.createErrorHandler(template);

        var record = new ConsumerRecord<String, Object>("product-events", 0, 5L, "k", "v");
        boolean handled = handler.handleOne(
                failure, record, mock(Consumer.class), mock(MessageListenerContainer.class));

        assertThat(handled).isTrue();
        var sent = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo("product-events.DLT");
    }

    private void assertRetriedBeforeRecovery(Exception failure) {
        KafkaTemplate<String, Object> template = successfulTemplate();
        DefaultErrorHandler handler = config.createErrorHandler(template, new FixedBackOff(0L, 1L));
        var record = new ConsumerRecord<String, Object>("product-events", 0, 5L, "k", "v");
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);

        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        verify(template, never()).send(any(ProducerRecord.class));

        assertThat(handler.handleOne(failure, record, consumer, container)).isTrue();
        verify(template, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("Should keep unexpected exceptions retryable and dead-letter them on exhaustion")
    void shouldRetryThenDeadLetterUnexpectedFailures() {
        KafkaTemplate<String, Object> template = successfulTemplate();
        DefaultErrorHandler handler = config.createErrorHandler(template, new FixedBackOff(0L, 1L));
        var record = new ConsumerRecord<String, Object>("product-events", 0, 7L, "k", "v");
        var failure = new IllegalStateException("OpenSearch unavailable");
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);

        boolean firstAttemptRecovered = handler.handleOne(failure, record, consumer, container);
        verify(template, never()).send(any(ProducerRecord.class));
        assertThat(firstAttemptRecovered).isFalse();

        boolean exhaustedRecovered = handler.handleOne(failure, record, consumer, container);
        assertThat(exhaustedRecovered).isTrue();
        verify(template, times(1)).send(any(ProducerRecord.class));
    }

    private KafkaTemplate<String, Object> successfulTemplate() {
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        return template;
    }
}
