package dev.jahlopez.productsearch.listener;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.repository.ProcessedEventEntity;
import dev.jahlopez.productsearch.repository.ProcessedEventRepository;
import dev.jahlopez.productsearch.service.ProductSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

        if (event == null) {
            log.warn("Received null ProductEvent payload at offset: {}", offset);
            return;
        }

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

        if (event == null || event.eventType() == null || event.productId() == null || event.eventId() == null) {
            log.warn("Received invalid or incomplete ProductEvent payload for search indexing.");
            return;
        }

        UUID eventId = event.eventId();
        String aggregateId = String.valueOf(event.productId());
        long incomingVersion = event.version() != null ? event.version() : 0L;

        log.info("Indexing ProductEvent [{}] (eventId={}, version={}) for productId: {} in OpenSearch",
                event.eventType(), eventId, incomingVersion, aggregateId);

        // 1. Idempotency Check (Drop if eventId has already been processed)
        if (processedEventRepository.existsById(eventId)) {
            log.info("Duplicate event ignored at inbox check: eventId={}", eventId);
            return;
        }

        // 2. Out-of-Order Check via Inbox Table
        Long maxProcessedVersion = processedEventRepository.findHighestProcessedVersion(AGGREGATE_TYPE, aggregateId).orElse(0L);
        if (incomingVersion <= maxProcessedVersion) {
            log.warn("Stale or out-of-order event ignored: aggregateId={}, incomingVersion={}, currentMaxVersion={}",
                    aggregateId, incomingVersion, maxProcessedVersion);
            return;
        }

        // 3. Fallback Document Version Guard in OpenSearch
        if (event.eventType() == ProductEvent.EventType.CREATED || event.eventType() == ProductEvent.EventType.UPDATED) {
            Optional<ProductDocument> existingDocOpt = productSearchService.findById(aggregateId);
            if (existingDocOpt.isPresent()) {
                ProductDocument existingDoc = existingDocOpt.get();
                Long existingVersion = existingDoc.getVersion() != null ? existingDoc.getVersion() : 0L;

                if (incomingVersion <= existingVersion) {
                    log.warn("Stale event detected in OpenSearch document check for productId: {}. Incoming ({}) <= Existing ({}). Discarding event.",
                            aggregateId, incomingVersion, existingVersion);
                    return;
                }
            }
        }

        // 4. Process Indexing / Deletion Business Logic
        switch (event.eventType()) {
            case CREATED, UPDATED -> {
                ProductDocument document = mapToDocument(event);
                productSearchService.indexProduct(document);
                log.info("Successfully indexed product ID: {} (version: {}) in OpenSearch", aggregateId, incomingVersion);
            }
            case DELETED -> {
                productSearchService.deleteProductIndex(aggregateId);
                log.info("Successfully removed product ID: {} from OpenSearch", aggregateId);
            }
            default -> {
                log.warn("Unhandled event type [{}] encountered for productId: {}", event.eventType(), aggregateId);
                return;
            }
        }

        // 5. Record Processed Event in Inbox Table
        ProcessedEventEntity processedEvent = ProcessedEventEntity.builder()
                .eventId(eventId)
                .aggregateType(AGGREGATE_TYPE)
                .aggregateId(aggregateId)
                .aggregateVersion(incomingVersion)
                .eventType(event.eventType().name())
                .status("SUCCESS")
                .kafkaTopic(topic)
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .build();

        processedEventRepository.save(processedEvent);
        log.info("Recorded eventId={} in inbox table for aggregateId={}", eventId, aggregateId);
    }

    /**
     * Dedicated consumer listener to monitor, audit, and log messages landing in the Dead Letter Queue (DLT).
     *
     * @param event        the failed product event payload recovered from the DLT
     * @param errorMessage the root-cause exception message associated with the failure
     */
    @KafkaListener(
            topics = "${app.kafka.topic.product-events:product-events}.DLT",
            groupId = "product-search-dlq-group"
    )
    public void handleProductEventDlq(
            ProductEvent event,
            @Header(name = "kafka_dlt-exception-message", defaultValue = "Unknown") String errorMessage) {
        String sku = (event != null && event.sku() != null) ? event.sku() : "UNKNOWN";
        log.error("⚠️ Dead Letter Queue Alert: Message for SKU '{}' (eventId={}) failed processing. Reason: {}",
                sku, event != null ? event.eventId() : "N/A", errorMessage);
    }

    /**
     * Maps a ProductEvent payload to an OpenSearch ProductDocument projection, including version metadata.
     *
     * @param event the product event to map
     * @return the constructed ProductDocument
     */
    private ProductDocument mapToDocument(ProductEvent event) {
        return ProductDocument.builder()
                .id(String.valueOf(event.productId()))
                .sku(event.sku())
                .name(event.name())
                .description(event.description())
                .price(event.price())
                .stockQuantity(event.stockQuantity())
                .version(event.version())
                .build();
    }
}