package dev.jahlopez.productsearch.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Data Transfer Object (DTO) capturing advanced search criteria, keyword filtering,
 * price ranges, and faceted attributes for OpenSearch queries.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Search query criteria payload for filtering and querying product documents.")
public class ProductSearchQueryDto {

    @Schema(description = "Full-text search keyword matching product name or description", example = "Mechanical Keyboard")
    private String keyword;

    @Schema(description = "Category name or identifier for filtering", example = "Electronics")
    private String category;

    @Schema(description = "Minimum price filter boundary", example = "50.00")
    @DecimalMin(value = "0.00", message = "Minimum price must be greater than or equal to 0")
    private BigDecimal minPrice;

    @Schema(description = "Maximum price filter boundary", example = "500.00")
    @DecimalMin(value = "0.00", message = "Maximum price must be greater than or equal to 0")
    private BigDecimal maxPrice;

    @Schema(description = "Flag to restrict search results strictly to items with available inventory stock", example = "true")
    private Boolean inStockOnly;
}