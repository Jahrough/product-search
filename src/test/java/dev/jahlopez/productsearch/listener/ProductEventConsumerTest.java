package dev.jahlopez.productsearch.listener;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.repository.ProcessedEventEntity;
import dev.jahlopez.productsearch.repository.ProcessedEventRepository;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit test suite for {@link ProductEventConsumer}.
 * <p>
 * Validates consumer-side business logic without external infrastructure overhead,
 * including inbox deduplication (idempotency), stale event/version sequence guards,
 * deletion lifecycle events, and simulated failure (poison-pill) behavior for DLQ routing.
 */
@ExtendWith(MockitoExtension.class)
class ProductEventConsumerTest {

    @Mock
    private ProductSearchService productSearchService;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @InjectMocks
    private ProductEventConsumer productEventConsumer;

    private ProductEvent sampleEvent;
    private ProductDocument existingDocument;

    @BeforeEach
    void setUp() {
        sampleEvent = ProductEvent.builder()
                .eventId(UUID.randomUUID())
                .version(2L)
                .productId("prod-123")
                .sku("SKU-TEST-01")
                .name("Test Mechanical Keyboard")
                .description("RGB backlit mechanical keyboard")
                .price(new BigDecimal("129.99"))
                .stockQuantity(50)
                .eventType(ProductEvent.EventType.UPDATED)
                .timestamp(LocalDateTime.now())
                .build();

        existingDocument = ProductDocument.builder()
                .id("prod-123")
                .sku("SKU-TEST-01")
                .name("Old Mechanical Keyboard")
                .price(new BigDecimal("119.99"))
                .stockQuantity(40)
                .version(2L)
                .build();
    }

    /**
     * Verifies that incoming events with higher aggregate versions trigger search index
     * updates and are recorded in the processed_events inbox table.
     */
    @Test
    @DisplayName("Should successfully index product when CREATED or UPDATED event arrives with newer version")
    void shouldIndexProductOnNewerVersion() {
        // Arrange: Existing search document has lower version (1L), incoming event is 2L
        existingDocument.setVersion(1L);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123"))
                .thenReturn(Optional.of(1L));
        when(productSearchService.findById("prod-123"))
                .thenReturn(Optional.of(existingDocument));

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert: Indexing service is invoked and event ID is persisted in inbox table
        verify(productSearchService).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).deleteProductIndex(anyString());
    }

    /**
     * Verifies that incoming events with equal or lower aggregate versions are discarded without
     * updating OpenSearch or saving duplicates in the inbox table.
     */
    @Test
    @DisplayName("Should discard event and skip indexing when incoming version is older or equal (stale update guard)")
    void shouldDiscardStaleOrDuplicateEvent() {
        // Arrange: Inbox table already saw version 2L, incoming version is 2L (stale/equal)
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123"))
                .thenReturn(Optional.of(2L));

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert: Neither indexing nor inbox persistence occurs
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    /**
     * Verifies that duplicate messages (sharing an existing `eventId`) are ignored immediately.
     */
    @Test
    @DisplayName("Should ignore event if eventId already exists in processed_events inbox table")
    void shouldIgnoreDuplicateEventId() {
        // Arrange: Event ID is already recorded in inbox persistence
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(true);

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert: Processing is short-circuited immediately
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    /**
     * Verifies that receiving a `DELETED` product event removes the document from OpenSearch
     * and records the deletion version in the inbox table.
     */
    @Test
    @DisplayName("Should successfully delete product index when DELETED event arrives")
    void shouldDeleteProductOnDeletedEvent() {
        // Arrange: Build valid deletion lifecycle event
        ProductEvent deleteEvent = ProductEvent.builder()
                .eventId(UUID.randomUUID())
                .version(3L)
                .productId("prod-123")
                .sku("SKU-TEST-01")
                .eventType(ProductEvent.EventType.DELETED)
                .timestamp(LocalDateTime.now())
                .build();

        when(processedEventRepository.existsById(deleteEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123"))
                .thenReturn(Optional.of(2L));

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(deleteEvent, "product-events", 0, 101L);

        // Assert: OpenSearch index is deleted and inbox record is updated
        verify(productSearchService).deleteProductIndex("prod-123");
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
    }

    /**
     * Verifies that processing a designated poison-pill event throws a RuntimeException to trigger
     * Spring Kafka retries and ultimate Dead Letter Queue (DLQ) routing.
     */
    @Test
    @DisplayName("Should throw exception when poison-pill SKU is processed to trigger DLQ routing")
    void shouldThrowExceptionOnPoisonPillSku() {
        // Arrange: Build poison-pill event payload
        ProductEvent poisonPillEvent = ProductEvent.builder()
                .eventId(UUID.randomUUID())
                .version(1L)
                .productId("prod-poison")
                .sku("POISON-PILL-SKU")
                .name("Poison Product")
                .eventType(ProductEvent.EventType.CREATED)
                .timestamp(LocalDateTime.now())
                .build();

        when(processedEventRepository.existsById(poisonPillEvent.eventId())).thenReturn(false);

        // Act & Assert: Exception is thrown to initiate error handling and recovery pipeline
        assertThatThrownBy(() -> productEventConsumer.handleProductEvent(poisonPillEvent, 0L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated processing failure - triggering retry & DLQ");
    }
}