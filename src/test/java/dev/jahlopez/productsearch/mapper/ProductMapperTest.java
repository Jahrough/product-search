package dev.jahlopez.productsearch.mapper;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.event.ProductEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test suite for the MapStruct-generated {@link ProductMapper} implementation.
 * <p>
 * Validates bi-directional mappings between OpenSearch product documents, API response DTOs,
 * and incoming Kafka event payloads without requiring a full Spring application context.
 */
class ProductMapperTest {

    private final ProductMapper productMapper = Mappers.getMapper(ProductMapper.class);

    /**
     * Verifies that an OpenSearch {@link ProductDocument} is correctly projected onto
     * a {@link ProductResponseDto} for client-facing API responses.
     */
    @Test
    @DisplayName("Should correctly map ProductDocument to ProductResponseDto")
    void shouldMapDocumentToResponseDto() {
        // Arrange: Construct sample search domain document
        ProductDocument document = ProductDocument.builder()
                .id("prod-789")
                .sku("SKU-MAP-01")
                .name("Mechanical Keyboard")
                .description("Hot-swappable mechanical keyboard")
                .price(new BigDecimal("159.99"))
                .stockQuantity(25)
                .version(1L)
                .build();

        // Act: Invoke MapStruct mapping interface
        ProductResponseDto dto = productMapper.toResponseDto(document);

        // Assert: Validate all fields match accurately
        assertThat(dto).isNotNull();
        assertThat(dto.getId()).isEqualTo(document.getId());
        assertThat(dto.getSku()).isEqualTo(document.getSku());
        assertThat(dto.getName()).isEqualTo(document.getName());
        assertThat(dto.getDescription()).isEqualTo(document.getDescription());
        assertThat(dto.getPrice()).isEqualByComparingTo(document.getPrice());
        assertThat(dto.getStockQuantity()).isEqualTo(document.getStockQuantity());
    }

    /**
     * Verifies that a client-submitted {@link ProductResponseDto} is correctly mapped
     * into a {@link ProductDocument}, ensuring version fields are appropriately ignored/omitted.
     */
    @Test
    @DisplayName("Should correctly map ProductResponseDto to ProductDocument")
    void shouldMapResponseDtoToDocument() {
        // Arrange: Construct sample request/response DTO payload
        ProductResponseDto dto = ProductResponseDto.builder()
                .id("prod-789")
                .sku("SKU-MAP-01")
                .name("Mechanical Keyboard")
                .description("Hot-swappable mechanical keyboard")
                .price(new BigDecimal("159.99"))
                .stockQuantity(25)
                .build();

        // Act: Invoke MapStruct mapping interface
        ProductDocument document = productMapper.toDocument(dto);

        // Assert: Validate structural fields match and unowned version is ignored
        assertThat(document).isNotNull();
        assertThat(document.getId()).isEqualTo(dto.getId());
        assertThat(document.getSku()).isEqualTo(dto.getSku());
        assertThat(document.getName()).isEqualTo(dto.getName());
        assertThat(document.getDescription()).isEqualTo(dto.getDescription());
        assertThat(document.getPrice()).isEqualByComparingTo(dto.getPrice());
        assertThat(document.getStockQuantity()).isEqualTo(dto.getStockQuantity());
    }

    /**
     * Verifies that an incoming Kafka {@link ProductEvent} is correctly mapped into
     * a search projection {@link ProductDocument}, ensuring identifiers and versions align.
     */
    @Test
    @DisplayName("Should correctly map ProductEvent to ProductDocument projection")
    void shouldMapEventToDocument() {
        // Arrange: Construct sample Kafka lifecycle event payload
        ProductEvent event = ProductEvent.builder()
                .eventId(UUID.randomUUID())
                .version(2L)
                .productId("prod-789")
                .sku("SKU-MAP-01")
                .name("Mechanical Keyboard")
                .description("Hot-swappable mechanical keyboard")
                .price(new BigDecimal("159.99"))
                .stockQuantity(25)
                .eventType(ProductEvent.EventType.UPDATED)
                .timestamp(LocalDateTime.now())
                .build();

        // Act: Invoke MapStruct mapping interface
        ProductDocument document = productMapper.toDocument(event);

        // Assert: Validate event attributes correctly map to document fields
        assertThat(document).isNotNull();
        assertThat(document.getId()).isEqualTo(event.productId());
        assertThat(document.getSku()).isEqualTo(event.sku());
        assertThat(document.getName()).isEqualTo(event.name());
        assertThat(document.getVersion()).isEqualTo(event.version());
    }
}