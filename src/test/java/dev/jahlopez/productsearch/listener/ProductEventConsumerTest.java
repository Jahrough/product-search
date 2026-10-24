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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

    @Test
    @DisplayName("Should successfully index product when CREATED or UPDATED event arrives with newer version")
    void shouldIndexProductOnNewerVersion() {
        // Arrange
        existingDocument.setVersion(1L);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123")).thenReturn(Optional.of(1L));
        when(productSearchService.findById("prod-123")).thenReturn(Optional.of(existingDocument));

        // Act
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert
        verify(productSearchService).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).deleteProductIndex(anyString());
    }

    @Test
    @DisplayName("Should discard event and skip indexing when incoming version is older or equal (stale update guard)")
    void shouldDiscardStaleOrDuplicateEvent() {
        // Arrange
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        // Inbox table already saw version 2L, incoming version is 2L (stale/equal)
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123")).thenReturn(Optional.of(2L));

        // Act
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should ignore event if eventId already exists in processed_events inbox table")
    void shouldIgnoreDuplicateEventId() {
        // Arrange
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(true);

        // Act
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should successfully delete product index when DELETED event arrives")
    void shouldDeleteProductOnDeletedEvent() {
        // Arrange
        ProductEvent deleteEvent = ProductEvent.builder()
                .eventId(UUID.randomUUID())
                .version(3L)
                .productId("prod-123")
                .sku("SKU-TEST-01")
                .eventType(ProductEvent.EventType.DELETED)
                .timestamp(LocalDateTime.now())
                .build();

        when(processedEventRepository.existsById(deleteEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "prod-123")).thenReturn(Optional.of(2L));

        // Act
        productEventConsumer.handleSearchIndexingEvent(deleteEvent, "product-events", 0, 101L);

        // Assert
        verify(productSearchService).deleteProductIndex("prod-123");
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
    }

    @Test
    @DisplayName("Should throw exception when poison-pill SKU is processed to trigger DLQ routing")
    void shouldThrowExceptionOnPoisonPillSku() {
        // Arrange
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

        // Act & Assert
        assertThatThrownBy(() -> productEventConsumer.handleProductEvent(poisonPillEvent, 0L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated processing failure - triggering retry & DLQ");
    }
}