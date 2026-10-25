package dev.jahlopez.productsearch.service;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import dev.jahlopez.productsearch.exception.ResourceNotFoundException;
import dev.jahlopez.productsearch.mapper.ProductMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
@CacheConfig(cacheNames = "products")
public class ProductSearchService {

    private static final String INDEX_NAME = "products";
    private static final String FIELD_SKU = "sku.keyword";
    private static final String FIELD_CATEGORY = "category.keyword";

    private static final String SERVICE_DEGRADED_MESSAGE =
            "Product search service is temporarily degraded. Please try again shortly.";

    private final OpenSearchClient openSearchClient;
    private final ProductMapper productMapper;

    /**
     * Searches products using optional keyword and category filters.
     *
     * @param queryDto search criteria
     * @param pageable pagination parameters
     * @return a page of matching products
     */
    @CircuitBreaker(
            name = "openSearchService",
            fallbackMethod = "searchProductsFallback"
    )
    public Page<ProductResponseDto> searchProducts(
            ProductSearchQueryDto queryDto,
            Pageable pageable
    ) {
        validateSearchRequest(queryDto, pageable);

        int offset = calculateOffset(pageable);

        log.debug(
                "Searching products: keyword='{}', category='{}', page={}, size={}",
                queryDto.getKeyword(),
                queryDto.getCategory(),
                pageable.getPageNumber(),
                pageable.getPageSize()
        );

        try {
            SearchResponse<ProductDocument> response = openSearchClient.search(
                    request -> request
                            .index(INDEX_NAME)
                            .from(offset)
                            .size(pageable.getPageSize())
                            .query(query -> query.bool(bool -> {
                                String keyword = normalize(queryDto.getKeyword());
                                String category = normalize(queryDto.getCategory());

                                if (hasText(keyword)) {
                                    bool.must(must -> must.multiMatch(
                                            multiMatch -> multiMatch
                                                    .query(keyword)
                                                    .fields(
                                                            "name",
                                                            "description",
                                                            "sku"
                                                    )
                                    ));
                                }

                                if (hasText(category)) {
                                    bool.filter(filter -> filter.term(
                                            term -> term
                                                    .field(FIELD_CATEGORY)
                                                    .value(FieldValue.of(category))
                                    ));
                                }

                                return bool;
                            })),
                    ProductDocument.class
            );

            List<ProductResponseDto> products = response.hits()
                    .hits()
                    .stream()
                    .map(hit -> hit.source())
                    .filter(document -> document != null)
                    .map(productMapper::toResponseDto)
                    .toList();

            long totalHits = response.hits().total() != null
                    ? response.hits().total().value()
                    : products.size();

            return new PageImpl<>(products, pageable, totalHits);

        } catch (IOException exception) {
            throw openSearchFailure(
                    "Product search failed",
                    "Product search is temporarily unavailable.",
                    exception
            );
        }
    }

    /**
     * Gracefully degrades search requests when OpenSearch is unavailable.
     */
    private Page<ProductResponseDto> searchProductsFallback(
            ProductSearchQueryDto queryDto,
            Pageable pageable,
            Throwable cause
    ) {
        log.warn(
                "Product search fallback activated: {}",
                cause.getMessage()
        );

        return Page.empty(pageable);
    }

    /**
     * Retrieves a product by its OpenSearch document ID.
     *
     * @param id product document ID
     * @return product details
     */
    @Cacheable(key = "'id:' + #id")
    @CircuitBreaker(
            name = "openSearchService",
            fallbackMethod = "getProductByIdFallback"
    )
    public ProductResponseDto getProductById(String id) {
        String productId = normalizeRequiredIdentifier(id, "Product ID");

        ProductDocument document = findById(productId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Product not found with ID: " + productId
                ));

        return productMapper.toResponseDto(document);
    }

    /**
     * Preserves expected not-found responses instead of masking them as outages.
     */
    private ProductResponseDto getProductByIdFallback(
            String id,
            Throwable cause
    ) {
        if (cause instanceof ResourceNotFoundException notFound) {
            throw notFound;
        }

        log.error(
                "Product lookup by ID failed for '{}': {}",
                id,
                cause.getMessage(),
                cause
        );

        throw new IllegalStateException(SERVICE_DEGRADED_MESSAGE, cause);
    }

    /**
     * Retrieves a product by SKU.
     *
     * @param sku product stock keeping unit
     * @return product details
     */
    @Cacheable(key = "'sku:' + #sku")
    @CircuitBreaker(
            name = "openSearchService",
            fallbackMethod = "getProductBySkuFallback"
    )
    public ProductResponseDto getProductBySku(String sku) {
        String normalizedSku = normalizeRequiredIdentifier(sku, "SKU");

        log.debug("Looking up product by SKU: {}", normalizedSku);

        try {
            SearchResponse<ProductDocument> response = openSearchClient.search(
                    request -> request
                            .index(INDEX_NAME)
                            .size(1)
                            .query(query -> query.term(
                                    term -> term
                                            .field(FIELD_SKU)
                                            .value(FieldValue.of(normalizedSku))
                            )),
                    ProductDocument.class
            );

            return response.hits()
                    .hits()
                    .stream()
                    .map(hit -> hit.source())
                    .filter(document -> document != null)
                    .findFirst()
                    .map(productMapper::toResponseDto)
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "Product not found with SKU: " + normalizedSku
                    ));

        } catch (IOException exception) {
            throw openSearchFailure(
                    "SKU lookup failed for " + normalizedSku,
                    "Product lookup is temporarily unavailable.",
                    exception
            );
        }
    }

    /**
     * Handles SKU lookup failures without masking expected not-found responses.
     */
    private ProductResponseDto getProductBySkuFallback(
            String sku,
            Throwable cause
    ) {
        if (cause instanceof ResourceNotFoundException notFound) {
            throw notFound;
        }

        log.error(
                "Product lookup fallback activated for SKU '{}': {}",
                sku,
                cause.getMessage(),
                cause
        );

        throw new IllegalStateException(SERVICE_DEGRADED_MESSAGE, cause);
    }

    /**
     * Creates or replaces a product document in OpenSearch.
     * Intended for use by the Kafka event consumer.
     */
    @CacheEvict(value = "products", allEntries = true)
    public void indexProduct(ProductDocument document) {
        validateDocument(document);

        try {
            IndexRequest<ProductDocument> request = IndexRequest.of(
                    index -> index
                            .index(INDEX_NAME)
                            .id(document.getId())
                            .document(document)
            );

            openSearchClient.index(request);

            log.info(
                    "Indexed product ID {} in OpenSearch index {}",
                    document.getId(),
                    INDEX_NAME
            );

        } catch (IOException exception) {
            throw openSearchFailure(
                    "Product indexing failed for ID " + document.getId(),
                    "Product indexing failed.",
                    exception
            );
        }
    }

    /**
     * Deletes a product document from OpenSearch.
     * Intended for use by the Kafka event consumer.
     */
    @CacheEvict(value = "products", key = "'id:' + #id")
    public void deleteProductIndex(String id) {
        String productId = normalizeRequiredIdentifier(id, "Product ID");

        try {
            DeleteRequest request = DeleteRequest.of(
                    delete -> delete
                            .index(INDEX_NAME)
                            .id(productId)
            );

            openSearchClient.delete(request);

            log.info(
                    "Deleted product ID {} from OpenSearch index {}",
                    productId,
                    INDEX_NAME
            );

        } catch (IOException exception) {
            throw openSearchFailure(
                    "Product deletion failed for ID " + productId,
                    "Product deletion failed.",
                    exception
            );
        }
    }

    /**
     * Finds a product document by ID.
     *
     * @param id product document ID
     * @return an empty optional if the document does not exist
     */
    public Optional<ProductDocument> findById(String id) {
        String productId = normalizeRequiredIdentifier(id, "Product ID");

        try {
            GetResponse<ProductDocument> response = openSearchClient.get(
                    request -> request
                            .index(INDEX_NAME)
                            .id(productId),
                    ProductDocument.class
            );

            return response.found()
                    ? Optional.ofNullable(response.source())
                    : Optional.empty();

        } catch (IOException exception) {
            throw openSearchFailure(
                    "Product retrieval failed for ID " + productId,
                    "Product retrieval is temporarily unavailable.",
                    exception
            );
        }
    }

    private void validateSearchRequest(
            ProductSearchQueryDto queryDto,
            Pageable pageable
    ) {
        if (queryDto == null) {
            throw new IllegalArgumentException(
                    "Search query must not be null."
            );
        }

        if (pageable == null || pageable.isUnpaged()) {
            throw new IllegalArgumentException(
                    "A paginated Pageable is required."
            );
        }

        if (pageable.getPageSize() <= 0) {
            throw new IllegalArgumentException(
                    "Page size must be greater than zero."
            );
        }
    }

    private int calculateOffset(Pageable pageable) {
        try {
            return Math.toIntExact(pageable.getOffset());
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(
                    "Requested page offset is too large.",
                    exception
            );
        }
    }

    private void validateDocument(ProductDocument document) {
        if (document == null) {
            throw new IllegalArgumentException(
                    "Product document must not be null."
            );
        }

        normalizeRequiredIdentifier(
                document.getId(),
                "Product document ID"
        );
    }

    private String normalizeRequiredIdentifier(
            String value,
            String fieldName
    ) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(
                    fieldName + " must not be null or blank."
            );
        }

        return value.trim();
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private IllegalStateException openSearchFailure(
            String logMessage,
            String publicMessage,
            IOException exception
    ) {
        log.error(logMessage, exception);
        return new IllegalStateException(publicMessage, exception);
    }
}