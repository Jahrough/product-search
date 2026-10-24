package dev.jahlopez.productsearch.controller;

import dev.jahlopez.productsearch.config.swagger.ApiReadResponses;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import dev.jahlopez.productsearch.service.ProductSearchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/products/search")
@RequiredArgsConstructor
@Tag(
        name = "Product Search API",
        description = "Search product catalog documents by keyword, category, or SKU."
)
@SecurityRequirement(name = "BearerAuth")
public class ProductSearchController {

    private final ProductSearchService productSearchService;

    /**
     * Searches products using keyword and category filters.
     *
     * @param queryDto search criteria
     * @param pageable pagination and sorting parameters
     * @return matching products
     */
    @Operation(
            summary = "Search products",
            description = "Searches the OpenSearch product index using optional keyword and category filters."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Search results retrieved successfully",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = Page.class)
            )
    )
    @ApiReadResponses
    @PostMapping
    public ResponseEntity<Page<ProductResponseDto>> searchProducts(
            @Valid @RequestBody ProductSearchQueryDto queryDto,
            @ParameterObject Pageable pageable
    ) {
        Page<ProductResponseDto> results =
                productSearchService.searchProducts(queryDto, pageable);

        return ResponseEntity.ok(results);
    }

    /**
     * Retrieves a product by its OpenSearch document ID.
     *
     * @param id product document ID
     * @return product details
     */
    @Operation(
            summary = "Get product by ID",
            description = "Retrieves a product by its unique document ID."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Product retrieved successfully",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = ProductResponseDto.class)
            )
    )
    @ApiReadResponses
    @GetMapping("/{id}")
    public ResponseEntity<ProductResponseDto> getProductById(
            @Parameter(description = "Unique product document ID", required = true)
            @PathVariable String id
    ) {
        return ResponseEntity.ok(
                productSearchService.getProductById(id)
        );
    }

    /**
     * Retrieves a product by its SKU.
     *
     * @param sku product stock keeping unit
     * @return product details
     */
    @Operation(
            summary = "Get product by SKU",
            description = "Retrieves a product using its unique SKU."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Product retrieved successfully",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = ProductResponseDto.class)
            )
    )
    @ApiReadResponses
    @GetMapping("/sku/{sku}")
    public ResponseEntity<ProductResponseDto> getProductBySku(
            @Parameter(description = "Unique product SKU", required = true)
            @PathVariable String sku
    ) {
        return ResponseEntity.ok(
                productSearchService.getProductBySku(sku)
        );
    }
}