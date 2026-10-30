package dev.jahlopez.productsearch.listener;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.event.ProductEvent;
import dev.jahlopez.productsearch.exception.InvalidProductEventException;
import dev.jahlopez.productsearch.exception.ProductEventConflictException;
import dev.jahlopez.productsearch.mapper.ProductMapper;
import dev.jahlopez.productsearch.repository.ProcessedEventEntity;
import dev.jahlopez.productsearch.repository.ProcessedEventRepository;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mapstruct.factory.Mappers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    @Spy
    private ProductMapper productMapper = Mappers.getMapper(ProductMapper.class);

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @InjectMocks
    private ProductEventConsumer productEventConsumer;

    private ProductEvent sampleEvent;
    private ProductDocument existingDocument;

    @BeforeEach
    void setUp() {
        sampleEvent = ProductEvent.builder()
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(2L)
                .productId(123L)
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
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(1L));
        when(productSearchService.findById("123"))
                .thenReturn(Optional.of(existingDocument));

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        // Assert: Indexing service is invoked and event ID is persisted in inbox table
        verify(productSearchService).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).deleteProductIndex(anyString());
    }

    @Test
    @DisplayName("Should fail null indexing payloads so Kafka recovery can route them to the DLT")
    void shouldRejectNullIndexingEvent() {
        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                null, "product-events", 0, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product event payload must not be null.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }

    @Test
    @DisplayName("Should fail create events without an SKU instead of acknowledging them")
    void shouldRejectCreateEventWithoutSku() {
        ProductEvent eventWithoutSku = ProductEvent.builder()
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(1L)
                .productId(123L)
                .name("Test Product")
                .price(BigDecimal.TEN)
                .stockQuantity(10)
                .eventType(ProductEvent.EventType.CREATED)
                .timestamp(LocalDateTime.now())
                .build();

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                eventWithoutSku, "product-events", 0, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product SKU is required for create and update events.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }

    @Test
    @DisplayName("Should fail events without an event type instead of acknowledging them")
    void shouldRejectEventWithoutEventType() {
        ProductEvent eventWithoutType = ProductEvent.builder()
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(1L)
                .productId(123L)
                .sku("SKU-TEST-01")
                .name("Test Product")
                .price(BigDecimal.TEN)
                .stockQuantity(10)
                .timestamp(LocalDateTime.now())
                .build();

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                eventWithoutType, "product-events", 0, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product event eventType is required.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }

    @Test
    @DisplayName("Should reject events with a missing version instead of defaulting it to zero")
    void shouldRejectEventWithoutVersion() {
        ProductEvent eventWithoutVersion = eventWithVersion(null);

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                eventWithoutVersion, "product-events", 0, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product event version must be greater than zero.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }

    @Test
    @DisplayName("Should reject version zero")
    void shouldRejectZeroVersion() {
        assertInvalidVersion(0L);
    }

    @Test
    @DisplayName("Should reject negative versions")
    void shouldRejectNegativeVersion() {
        assertInvalidVersion(-1L);
    }

    @Test
    @DisplayName("Should fail null general-listener payloads so Kafka recovery can route them to the DLT")
    void shouldRejectNullGeneralListenerEvent() {
        assertThatThrownBy(() -> productEventConsumer.handleProductEvent(null, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product event payload must not be null.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }

    @Test
    @DisplayName("Should propagate inbox failure after indexing so Kafka can retry the idempotent upsert")
    void shouldPropagateInboxSaveFailureForRetryAfterOpenSearchIndexing() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(processedEventRepository.save(any(ProcessedEventEntity.class)))
                .thenThrow(new IllegalStateException("Inbox database timeout"))
                .thenAnswer(invocation -> invocation.getArgument(0));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Inbox database timeout");

        // The listener must fail rather than acknowledge an event whose inbox
        // write did not complete; a redelivery safely repeats the deterministic upsert.
        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        var indexedDocument = org.mockito.ArgumentCaptor.forClass(ProductDocument.class);
        verify(productSearchService, times(2)).indexProduct(indexedDocument.capture());
        assertThat(indexedDocument.getAllValues().get(1))
                .usingRecursiveComparison()
                .isEqualTo(indexedDocument.getAllValues().get(0));
        assertThat(indexedDocument.getAllValues())
                .allSatisfy(document -> {
                    assertThat(document.getId()).isEqualTo("123");
                    assertThat(document.getVersion()).isEqualTo(sampleEvent.version());
                    assertThat(document.getSku()).isEqualTo(sampleEvent.sku());
                });
        verify(processedEventRepository, times(2)).save(any(ProcessedEventEntity.class));
    }

    /**
     * Verifies that incoming events with equal or lower aggregate versions fail without
     * updating OpenSearch or saving duplicates in the inbox table.
     */
    @Test
    @DisplayName("Should raise a non-retryable conflict when the inbox holds a different event for the same version")
    void shouldRejectStaleOrDuplicateAggregateVersion() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(2L));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(ProductEventConflictException.class)
                .hasMessageContaining("Event identity conflict")
                .hasMessageContaining("version=2");

        verifyNoInteractions(productSearchService);
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
        verify(processedEventRepository, never())
                .existsByAggregateTypeAndAggregateIdAndAggregateVersion(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("Should skip an older event as SKIPPED without touching OpenSearch")
    void shouldSkipOlderAggregateVersion() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(3L));
        when(processedEventRepository.existsByAggregateTypeAndAggregateIdAndAggregateVersion("Product", "123", 2L))
                .thenReturn(false);

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verifyNoInteractions(productSearchService);
        var saved = org.mockito.ArgumentCaptor.forClass(ProcessedEventEntity.class);
        verify(processedEventRepository).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo("SKIPPED");
        assertThat(saved.getValue().getEventId()).isEqualTo(sampleEvent.eventId());
    }

    @Test
    @DisplayName("Should not insert a SKIPPED row when the aggregate/version slot already exists")
    void shouldPreserveExistingInboxRowForStaleVersion() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(3L));
        when(processedEventRepository.existsByAggregateTypeAndAggregateIdAndAggregateVersion("Product", "123", 2L))
                .thenReturn(true);

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verifyNoInteractions(productSearchService);
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should skip an event older than the OpenSearch projection without overwriting it")
    void shouldSkipVersionOlderThanOpenSearchDocument() {
        existingDocument.setVersion(3L);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.of(existingDocument));

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        var saved = org.mockito.ArgumentCaptor.forClass(ProcessedEventEntity.class);
        verify(processedEventRepository).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo("SKIPPED");
    }

    @Test
    @DisplayName("Should record SUCCESS without re-indexing when OpenSearch already holds the equivalent version")
    void shouldAcknowledgeEquivalentStateAtEqualVersion() {
        existingDocument = productMapper.toDocument(sampleEvent);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.of(existingDocument));

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        var saved = org.mockito.ArgumentCaptor.forClass(ProcessedEventEntity.class);
        verify(processedEventRepository).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo("SUCCESS");
    }

    @Test
    @DisplayName("Should treat prices that differ only in scale as equivalent")
    void shouldComparePriceByValueNotScale() {
        existingDocument = productMapper.toDocument(sampleEvent);
        existingDocument.setPrice(new BigDecimal("129.990"));
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.of(existingDocument));

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
    }

    @Test
    @DisplayName("Should raise a conflict when OpenSearch holds the same version with different content")
    void shouldRejectDifferentStateAtEqualVersion() {
        existingDocument.setVersion(sampleEvent.version());
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.of(existingDocument));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(ProductEventConflictException.class)
                .hasMessageContaining("Product state conflict");

        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should index the first event when no OpenSearch document exists")
    void shouldIndexFirstEventWhenDocumentAbsent() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.empty());

        productEventConsumer.handleSearchIndexingEvent(sampleEvent, "product-events", 0, 100L);

        verify(productSearchService).indexProduct(any(ProductDocument.class));
        var saved = org.mockito.ArgumentCaptor.forClass(ProcessedEventEntity.class);
        verify(processedEventRepository).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo("SUCCESS");
    }

    @Test
    @DisplayName("Should recover on redelivery when OpenSearch write succeeded but the inbox save failed")
    void shouldRecoverWithoutReindexAfterInboxFailure() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(productMapper.toDocument(sampleEvent)));
        when(processedEventRepository.save(any(ProcessedEventEntity.class)))
                .thenThrow(new IllegalStateException("Inbox database timeout"))
                .thenAnswer(invocation -> invocation.getArgument(0));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(IllegalStateException.class);

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        verify(productSearchService, times(1)).indexProduct(any(ProductDocument.class));
        verify(processedEventRepository, times(2)).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should not let a reordered version 2 overwrite version 3")
    void shouldNotOverwriteNewerVersionWithReorderedEvent() {
        ProductEvent v3 = eventWithVersion(3L);
        ProductEvent v2 = eventWithVersion(2L);
        when(processedEventRepository.existsById(any(UUID.class))).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(3L));
        when(productSearchService.findById("123")).thenReturn(Optional.empty());

        productEventConsumer.handleSearchIndexingEvent(v3, "product-events", 0, 100L);
        productEventConsumer.handleSearchIndexingEvent(v2, "product-events", 0, 101L);

        var indexed = org.mockito.ArgumentCaptor.forClass(ProductDocument.class);
        verify(productSearchService, times(1)).indexProduct(indexed.capture());
        assertThat(indexed.getValue().getVersion()).isEqualTo(3L);
    }

    @Test
    @DisplayName("Should propagate infrastructure failures unchanged so they stay retryable")
    void shouldPropagateUnexpectedFailures() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.empty());
        doThrow(new IllegalStateException("OpenSearch unavailable"))
                .when(productSearchService).indexProduct(any(ProductDocument.class));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OpenSearch unavailable");

        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should not convert repository or service failures into invalid-event failures")
    void shouldNotWrapInfrastructureFailureAsInvalidEvent() {
        var databaseFailure = new org.springframework.dao.DataAccessResourceFailureException("PostgreSQL unavailable");
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenThrow(databaseFailure);

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isSameAs(databaseFailure)
                .isNotInstanceOf(InvalidProductEventException.class);

        reset(processedEventRepository);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        var serviceFailure = new IllegalStateException("OpenSearch unavailable");
        when(productSearchService.findById("123")).thenThrow(serviceFailure);

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isSameAs(serviceFailure)
                .isNotInstanceOf(InvalidProductEventException.class);
    }

    @Test
    @DisplayName("Should reject projections without a version because ordering cannot be verified")
    void shouldRejectUnversionedOpenSearchDocument() {
        existingDocument.setVersion(null);
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.empty());
        when(productSearchService.findById("123")).thenReturn(Optional.of(existingDocument));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has no version");

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

        // Duplicate records are acknowledged successfully without repeating side effects.
        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .doesNotThrowAnyException();

        // Assert: Processing is short-circuited immediately
        verifyNoInteractions(productSearchService);
        verify(processedEventRepository, never()).findHighestProcessedVersion(anyString(), anyString());
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should raise a conflict for an already-recorded aggregate version without overwriting the projection")
    void shouldRejectDuplicateAggregateVersionWithoutOpenSearchWrite() {
        when(processedEventRepository.existsById(sampleEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(sampleEvent.version()));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                sampleEvent, "product-events", 0, 100L))
                .isInstanceOf(ProductEventConflictException.class);

        verifyNoInteractions(productSearchService);
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
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(3L)
                .productId(123L)
                .sku("SKU-TEST-01")
                .eventType(ProductEvent.EventType.DELETED)
                .timestamp(LocalDateTime.now())
                .build();

        when(processedEventRepository.existsById(deleteEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123"))
                .thenReturn(Optional.of(2L));

        // Act: Invoke consumer message handler
        productEventConsumer.handleSearchIndexingEvent(deleteEvent, "product-events", 0, 101L);

        // Assert: OpenSearch index is deleted and inbox record is updated
        verify(productSearchService).deleteProductIndex("123");
        verify(processedEventRepository).save(any(ProcessedEventEntity.class));
        verify(productSearchService, never()).indexProduct(any(ProductDocument.class));
    }

    @Test
    @DisplayName("Should raise a conflict for DELETED events whose version equals the highest processed version")
    void shouldRejectDeleteWithEqualVersion() {
        ProductEvent deleteEvent = ProductEvent.builder()
                .schemaVersion(1).eventId(UUID.randomUUID()).version(2L).productId(123L)
                .sku("SKU-TEST-01").eventType(ProductEvent.EventType.DELETED)
                .timestamp(LocalDateTime.now()).build();
        when(processedEventRepository.existsById(deleteEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123")).thenReturn(Optional.of(2L));

        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                deleteEvent, "product-events", 0, 102L))
                .isInstanceOf(ProductEventConflictException.class);

        verify(productSearchService, never()).deleteProductIndex(anyString());
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
    }

    @Test
    @DisplayName("Should skip DELETED events whose version is lower than the highest processed version")
    void shouldDiscardStaleDelete() {
        ProductEvent deleteEvent = ProductEvent.builder()
                .schemaVersion(1).eventId(UUID.randomUUID()).version(1L).productId(123L)
                .sku("SKU-TEST-01").eventType(ProductEvent.EventType.DELETED)
                .timestamp(LocalDateTime.now()).build();
        when(processedEventRepository.existsById(deleteEvent.eventId())).thenReturn(false);
        when(processedEventRepository.findHighestProcessedVersion("Product", "123")).thenReturn(Optional.of(2L));

        assertThatCode(() -> productEventConsumer.handleSearchIndexingEvent(
                deleteEvent, "product-events", 0, 103L))
                .doesNotThrowAnyException();

        verify(productSearchService, never()).deleteProductIndex(any());
        var saved = org.mockito.ArgumentCaptor.forClass(ProcessedEventEntity.class);
        verify(processedEventRepository).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo("SKIPPED");
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
                .schemaVersion(1)
                .eventId(UUID.randomUUID())
                .version(1L)
                .productId(999L)
                .sku("POISON-PILL-SKU")
                .name("Poison Product")
                .price(BigDecimal.TEN)
                .stockQuantity(1)
                .eventType(ProductEvent.EventType.CREATED)
                .timestamp(LocalDateTime.now())
                .build();

        when(processedEventRepository.existsById(poisonPillEvent.eventId())).thenReturn(false);

        // Act & Assert: Exception is thrown to initiate error handling and recovery pipeline
        assertThatThrownBy(() -> productEventConsumer.handleProductEvent(poisonPillEvent, 0L))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated processing failure - triggering retry & DLQ");
    }

    private ProductEvent eventWithVersion(Long version) {
        return ProductEvent.builder()
                .schemaVersion(sampleEvent.schemaVersion())
                .eventId(UUID.randomUUID())
                .version(version)
                .productId(sampleEvent.productId())
                .sku(sampleEvent.sku())
                .name(sampleEvent.name())
                .description(sampleEvent.description())
                .price(sampleEvent.price())
                .stockQuantity(sampleEvent.stockQuantity())
                .eventType(sampleEvent.eventType())
                .timestamp(sampleEvent.timestamp())
                .build();
    }

    private void assertInvalidVersion(Long version) {
        assertThatThrownBy(() -> productEventConsumer.handleSearchIndexingEvent(
                eventWithVersion(version), "product-events", 0, 100L))
                .isInstanceOf(InvalidProductEventException.class)
                .hasMessage("Product event version must be greater than zero.");

        verifyNoInteractions(productSearchService, processedEventRepository);
    }
}