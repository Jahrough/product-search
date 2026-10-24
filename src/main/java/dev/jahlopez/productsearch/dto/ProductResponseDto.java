package dev.jahlopez.productsearch.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Data Transfer Object (DTO) representing product response details
 * returned by search and lookup endpoints.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Product response payload containing search item attributes.")
public class ProductResponseDto {

    @Schema(description = "Unique product document identifier", example = "prod-12345")
    private String id;

    @Schema(description = "Stock Keeping Unit (SKU)", example = "SKU-98765")
    private String sku;

    @Schema(description = "Product name", example = "Ergonomic Mechanical Keyboard")
    private String name;

    @Schema(description = "Product description", example = "Wireless mechanical keyboard with RGB backlighting.")
    private String description;

    @Schema(description = "Product price", example = "149.99")
    private BigDecimal price;

    @Schema(description = "Available inventory stock quantity", example = "42")
    private Integer stockQuantity;
}