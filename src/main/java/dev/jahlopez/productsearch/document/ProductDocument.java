package dev.jahlopez.productsearch.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * OpenSearch document representation of a catalog product.
 *
 * Supports full-text search, exact-match filtering, and version-aware
 * synchronization from catalog events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDocument {

    /**
     * Unique product identifier, matching the catalog product ID.
     */
    private String id;

    /**
     * Stock Keeping Unit used for exact product lookup.
     */
    private String sku;

    /**
     * Product name used for full-text search.
     */
    private String name;

    /**
     * Product description used for full-text search.
     */
    private String description;

    /**
     * Product price represented as a decimal value.
     */
    private BigDecimal price;

    /**
     * Available inventory quantity.
     */
    private Integer stockQuantity;

    /**
     * Monotonically increasing source version for this product.
     *
     * The version should originate from the catalog event or another
     * authoritative source, not be generated independently by the
     * search service.
     */
    private Long version;
}