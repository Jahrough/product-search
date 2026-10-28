package dev.jahlopez.productsearch.event;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Immutable data record representing a product lifecycle event (e.g., created, updated, deleted)
 * that gets published to the Kafka event stream and consumed by the product-search service,
 * complete with idempotency and versioning metadata.
 *
 * Uses Lombok's @Builder annotation to allow convenient and readable object creation.
 */
@Builder
public record ProductEvent(
        Integer schemaVersion,
        UUID eventId,
        Long version,
        Long productId,
        String sku,
        String name,
        String description,
        BigDecimal price,
        Integer stockQuantity,
        EventType eventType,
        LocalDateTime timestamp
) {
    /**
     * Defines the type of lifecycle action that occurred for the product.
     */
    public enum EventType {
        /** Indicates a new product has been created and added to the catalog. */
        CREATED,

        /** Indicates an existing product's details or pricing have been updated. */
        UPDATED,

        /** Indicates a product has been deleted or removed from the catalog. */
        DELETED
    }
}