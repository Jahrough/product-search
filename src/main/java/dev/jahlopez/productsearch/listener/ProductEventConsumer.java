package dev.jahlopez.productsearch.listener;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.exception.InvalidProductEventException;
import dev.jahlopez.productsearch.exception.ProductEventConflictException;
import dev.jahlopez.productsearch.mapper.ProductMapper;
import dev.jahlopez.productsearch.repository.ProcessedEventEntity;
import dev.jahlopez.productsearch.repository.ProcessedEventRepository;
import dev.jahlopez.productsearch.service.ProductSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Event-driven Kafka consumer responsible for processing product lifecycle events,
 * maintaining near real-time OpenSearch search projections with idempotency and version guards,
 * and auditing DLQ failures.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProductEventConsumer {

    private final ProductSearchService productSearchService;
    private final ProductMapper productMapper;
    private final ProcessedEventRepository processedEventRepository;

    private static final String POISON_PILL_SKU = "POISON-PILL-SKU";
    private static final String AGGREGATE_TYPE = "Product";

    /**
     * Handles product events for general service business logic and poison-pill testing.
     *
     * @param event  the incoming product event payload
     * @param offset the Kafka message offset for tracking and auditing
     */
    @KafkaListener(
            topics = "${app.kafka.topic.product-events:product-events}",
            groupId = "${spring.kafka.consumer.group-id:product-search-group}"
    )
    @Transactional
    public void handleProductEvent(
            ProductEvent event,
            @Header(name = "kafka_offset", defaultValue = "0") long offset) {

        validateEvent(event);

        // Idempotency check for general listener
        if (event.eventId() != null && processedEventRepository.existsById(event.eventId())) {
            log.info("Duplicate event ignored in handleProductEvent: eventId={}", event.eventId());
            return;
        }

        log.info("Processing product event [eventId={}, version={}] for SKU: {} at offset: {}",
                event.eventId(), event.version(), event.sku(), offset);

        if (POISON_PILL_SKU.equals(event.sku())) {
            log.error("Encountered poison-pill SKU: {}. Triggering exception for retry and DLQ routing.", event.sku());
            throw new RuntimeException("Simulated processing failure - triggering retry & DLQ");
        }

        log.info("Successfully processed product event for SKU: {}", event.sku());
    }

    /**
     * Handles product events specifically for near real-time OpenSearch search indexing and synchronization,
     * enforcing version guards and idempotent event processing via the PostgreSQL inbox table.
     *
     * @param event     the incoming product lifecycle event
     * @param topic     the Kafka topic
     * @param partition the Kafka partition
     * @param offset    the Kafka message offset
     */
    @KafkaListener(
            topics = "${app.kafka.topic.product-events:product-events}",
            groupId = "product-search-search-group"
    )
    @Transactional
    public void handleSearchIndexingEvent(
            ProductEvent event,
            @Header(name = "kafka_receivedTopic", defaultValue = "product-events") String topic,
            @Header(name = "kafka_receivedPartitionId", defaultValue = "0") int partition,
            @Header(name = "kafka_offset", defaultValue = "0") long offset) {

        validateEvent(event);

        UUID eventId = event.eventId();
        String aggregateId = String.valueOf(event.productId());
        long incomingVersion = event.version();

        log.info("Indexing ProductEvent [{}] (eventId={}, version={}) for productId: {} in OpenSearch",
                event.eventType(), eventId, incomingVersion, aggregateId);

        // 1. Idempotency Check (Drop if eventId has already been processed)
        if (processedEventRepository.existsById(eventId)) {
            log.info("Duplicate event ignored at inbox check: eventId={}", eventId);
            return;
        }

        // 2. Out-of-Order Check via Inbox Table
        Optional<Long> maxProcessedVersion =
                processedEventRepository.findHighestProcessedVersion(AGGREGATE_TYPE, aggregateId);
        if (maxProcessedVersion.isPresent()) {
            long maxVersion = maxProcessedVersion.get();
            if (incomingVersion < maxVersion) {
                log.warn("Stale product event skipped: aggregateId={}, incomingVersion={}, inboxMaxVersion={}, eventId={}",
                        aggregateId, incomingVersion, maxVersion, eventId);
                recordSkipped(event, topic, partition, offset);
                return;
            }
            if (incomingVersion == maxVersion) {
                // The inbox already holds a different event for this aggregate version.
                throw new ProductEventConflictException(
                        "Event identity conflict: aggregateId=" + aggregateId
                                + ", version=" + incomingVersion
                                + " already recorded in inbox with a different eventId; incoming eventId=" + eventId
                );
            }
        }

        // 3. Fallback Document Version Guard in OpenSearch
        if (event.eventType() == ProductEvent.EventType.CREATED || event.eventType() == ProductEvent.EventType.UPDATED) {
            Optional<ProductDocument> existingDocOpt = productSearchService.findById(aggregateId);
            if (existingDocOpt.isPresent()) {
                ProductDocument existingDoc = existingDocOpt.get();
                Long existingVersion = existingDoc.getVersion();
                if (existingVersion == null) {
                    throw new IllegalStateException(
                            "Cannot verify product event version because the indexed document has no version: "
                                    + aggregateId
                    );
                }

                if (incomingVersion < existingVersion) {
                    log.warn("Stale product event skipped: aggregateId={}, incomingVersion={}, openSearchVersion={}, eventId={}",
                            aggregateId, incomingVersion, existingVersion, eventId);
                    recordSkipped(event, topic, partition, offset);
                    return;
                }

                if (incomingVersion == existingVersion) {
                    if (!isEquivalentState(productMapper.toDocument(event), existingDoc)) {
                        throw new ProductEventConflictException(
                                "Product state conflict: aggregateId=" + aggregateId
                                        + ", version=" + incomingVersion
                                        + " is already indexed with different content; incoming eventId=" + eventId
                        );
                    }
                    // Recovery path: OpenSearch write succeeded earlier but the inbox write did not.
                    log.info("Version already applied in OpenSearch; recording inbox only: aggregateId={}, version={}, eventId={}",
                            aggregateId, incomingVersion, eventId);
                    processedEventRepository.save(buildInboxRecord(event, "SUCCESS", topic, partition, offset));
                    return;
                }
            }
        }

        // 4. Process Indexing / Deletion Business Logic
        switch (event.eventType()) {
            case CREATED, UPDATED -> {
                ProductDocument document = productMapper.toDocument(event);
                productSearchService.indexProduct(document);
                log.info("Successfully indexed product ID: {} (version: {}) in OpenSearch", aggregateId, incomingVersion);
            }
            case DELETED -> {
                productSearchService.deleteProductIndex(aggregateId);
                log.info("Successfully removed product ID: {} from OpenSearch", aggregateId);
            }
            default -> {
                throw new InvalidProductEventException(
                        "Unsupported product event type: " + event.eventType()
                );
            }
        }

        // 5. Record Processed Event in Inbox Table
        processedEventRepository.save(buildInboxRecord(event, "SUCCESS", topic, partition, offset));
        log.info("Recorded eventId={} in inbox table for aggregateId={}", eventId, aggregateId);
    }

    /**
     * Records a stale event as SKIPPED unless the unique (aggregate, version) slot is already
     * taken, in which case the existing inbox row is preserved untouched.
     */
    private void recordSkipped(ProductEvent event, String topic, int partition, long offset) {
        String aggregateId = String.valueOf(event.productId());
        if (processedEventRepository.existsByAggregateTypeAndAggregateIdAndAggregateVersion(
                AGGREGATE_TYPE, aggregateId, event.version())) {
            return;
        }
        processedEventRepository.save(buildInboxRecord(event, "SKIPPED", topic, partition, offset));
    }

    private ProcessedEventEntity buildInboxRecord(
            ProductEvent event, String status, String topic, int partition, long offset) {
        return ProcessedEventEntity.builder()
                .eventId(event.eventId())
                .aggregateType(AGGREGATE_TYPE)
                .aggregateId(String.valueOf(event.productId()))
                .aggregateVersion(event.version())
                .eventType(event.eventType().name())
                .status(status)
                .kafkaTopic(topic)
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .build();
    }

    // The document carries no eventId, so this proves equivalent state, not identical event identity.
    private boolean isEquivalentState(ProductDocument incoming, ProductDocument existing) {
        return Objects.equals(incoming.getSku(), existing.getSku())
                && Objects.equals(incoming.getName(), existing.getName())
                && Objects.equals(incoming.getDescription(), existing.getDescription())
                && Objects.equals(incoming.getStockQuantity(), existing.getStockQuantity())
                && (incoming.getPrice() == null
                        ? existing.getPrice() == null
                        : existing.getPrice() != null && incoming.getPrice().compareTo(existing.getPrice()) == 0);
    }

    private void validateEvent(ProductEvent event) {
        if (event == null) {
            throw new InvalidProductEventException("Product event payload must not be null.");
        }
        if (!Integer.valueOf(1).equals(event.schemaVersion())) {
            throw new InvalidProductEventException("Product event schemaVersion must be 1.");
        }
        if (event.eventId() == null) {
            throw new InvalidProductEventException("Product event eventId is required.");
        }
        if (event.version() == null || event.version() < 1) {
            throw new InvalidProductEventException("Product event version must be greater than zero.");
        }
        if (event.productId() == null || event.productId() < 1) {
            throw new InvalidProductEventException("Product event productId must be greater than zero.");
        }
        if (event.eventType() == null) {
            throw new InvalidProductEventException("Product event eventType is required.");
        }
        if (event.timestamp() == null) {
            throw new InvalidProductEventException("Product event timestamp is required.");
        }
        if (event.eventType() != ProductEvent.EventType.DELETED) {
            if (event.sku() == null || event.sku().isBlank()) {
                throw new InvalidProductEventException("Product SKU is required for create and update events.");
            }
            if (event.name() == null || event.name().isBlank()) {
                throw new InvalidProductEventException("Product name is required for create and update events.");
            }
            if (event.price() == null || event.price().signum() < 0) {
                throw new InvalidProductEventException("Product price must be non-negative for create and update events.");
            }
            if (event.stockQuantity() == null || event.stockQuantity() < 0) {
                throw new InvalidProductEventException("Product stockQuantity must be non-negative for create and update events.");
            }
        }
    }

    /**
     * Dedicated consumer listener to monitor, audit, and log messages landing in the Dead Letter Queue (DLT).
     *
     * @param event        the failed product event payload recovered from the DLT
     * @param errorMessage the root-cause exception message associated with the failure
     */
    @KafkaListener(
            topics = "${app.kafka.topic.product-events:product-events}.DLT",
            groupId = "product-search-dlq-group",
            containerFactory = "deadLetterKafkaListenerContainerFactory"
    )
    public void handleProductEventDlq(
            byte[] eventPayload,
            @Header(name = "kafka_dlt-exception-message", defaultValue = "Unknown") String errorMessage) {
        log.error("Product event sent to the dead-letter topic (payloadBytes={}, reason={})",
                eventPayload == null ? 0 : eventPayload.length, errorMessage);
    }

}