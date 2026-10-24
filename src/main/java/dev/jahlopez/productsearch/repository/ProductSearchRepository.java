package dev.jahlopez.productsearch.repository;

import dev.jahlopez.productsearch.document.ProductDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ProductSearchRepository {

    private static final String INDEX_NAME = "products";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_DESCRIPTION = "description";
    private static final String FIELD_SKU = "sku.keyword";

    private final OpenSearchClient openSearchClient;

    /**
     * Searches product names and descriptions with pagination.
     * Either field may match when both terms are supplied.
     */
    public Page<ProductDocument> findByNameContainingOrDescriptionContaining(
            String name,
            String description,
            Pageable pageable
    ) {
        validatePageable(pageable);

        String normalizedName = normalize(name);
        String normalizedDescription = normalize(description);

        boolean hasName = normalizedName != null;
        boolean hasDescription = normalizedDescription != null;

        if (!hasName && !hasDescription) {
            throw new IllegalArgumentException(
                    "At least one search term must be provided."
            );
        }

        int offset = calculateOffset(pageable);

        try {
            Query query = Query.of(q -> q.bool(b -> {
                if (hasName) {
                    b.should(s -> s.match(m -> m
                            .field(FIELD_NAME)
                            .query(FieldValue.of(normalizedName))));
                }

                if (hasDescription) {
                    b.should(s -> s.match(m -> m
                            .field(FIELD_DESCRIPTION)
                            .query(FieldValue.of(normalizedDescription))));
                }

                return b.minimumShouldMatch("1");
            }));

            SearchResponse<ProductDocument> response =
                    openSearchClient.search(s -> s
                                    .index(INDEX_NAME)
                                    .from(offset)
                                    .size(pageable.getPageSize())
                                    .query(query),
                            ProductDocument.class);

            List<ProductDocument> documents = response.hits().hits()
                    .stream()
                    .map(hit -> hit.source())
                    .filter(document -> document != null)
                    .toList();

            long totalHits = response.hits().total() != null
                    ? response.hits().total().value()
                    : documents.size();

            return new PageImpl<>(documents, pageable, totalHits);

        } catch (IOException exception) {
            throw openSearchFailure("Product search failed.", exception);
        }
    }

    /**
     * Returns a product by its OpenSearch document ID.
     */
    public Optional<ProductDocument> findById(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Product ID is required.");
        }

        try {
            GetResponse<ProductDocument> response = openSearchClient.get(
                    g -> g.index(INDEX_NAME).id(id.trim()),
                    ProductDocument.class
            );

            return response.found()
                    ? Optional.ofNullable(response.source())
                    : Optional.empty();

        } catch (IOException exception) {
            throw openSearchFailure("Failed to retrieve product by ID.", exception);
        }
    }

    /**
     * Finds a product by its exact SKU.
     * Requires sku.keyword to exist in the index mapping.
     */
    public Optional<ProductDocument> findBySku(String sku) {
        String normalizedSku = normalize(sku);

        if (normalizedSku == null) {
            throw new IllegalArgumentException("Product SKU is required.");
        }

        try {
            SearchResponse<ProductDocument> response =
                    openSearchClient.search(s -> s
                                    .index(INDEX_NAME)
                                    .size(1)
                                    .query(q -> q.term(t -> t
                                            .field(FIELD_SKU)
                                            .value(FieldValue.of(normalizedSku)))),
                            ProductDocument.class);

            return response.hits().hits()
                    .stream()
                    .map(hit -> hit.source())
                    .filter(document -> document != null)
                    .findFirst();

        } catch (IOException exception) {
            throw openSearchFailure("Failed to retrieve product by SKU.", exception);
        }
    }

    /**
     * Indexes or replaces a product document using its ID.
     */
    public ProductDocument save(ProductDocument document) {
        if (document == null) {
            throw new IllegalArgumentException("Product document is required.");
        }

        String id = normalize(document.getId());

        if (id == null) {
            throw new IllegalArgumentException(
                    "Product document ID is required."
            );
        }

        try {
            openSearchClient.index(IndexRequest.of(i -> i
                    .index(INDEX_NAME)
                    .id(id)
                    .document(document)));

            return document;

        } catch (IOException exception) {
            throw openSearchFailure("Failed to index product.", exception);
        }
    }

    /**
     * Returns all products in a page.
     */
    public Page<ProductDocument> findAll(Pageable pageable) {
        validatePageable(pageable);

        try {
            SearchResponse<ProductDocument> response =
                    openSearchClient.search(s -> s
                                    .index(INDEX_NAME)
                                    .from(calculateOffset(pageable))
                                    .size(pageable.getPageSize())
                                    .query(q -> q.matchAll(m -> m)),
                            ProductDocument.class);

            List<ProductDocument> documents = response.hits().hits()
                    .stream()
                    .map(hit -> hit.source())
                    .filter(document -> document != null)
                    .toList();

            long totalHits = response.hits().total() != null
                    ? response.hits().total().value()
                    : documents.size();

            return new PageImpl<>(documents, pageable, totalHits);

        } catch (IOException exception) {
            throw openSearchFailure("Failed to retrieve products.", exception);
        }
    }

    /**
     * Deletes a product document by ID.
     */
    public void deleteById(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Product ID is required.");
        }

        try {
            openSearchClient.delete(DeleteRequest.of(d -> d
                    .index(INDEX_NAME)
                    .id(id.trim())));

        } catch (IOException exception) {
            throw openSearchFailure("Failed to delete product.", exception);
        }
    }

    private void validatePageable(Pageable pageable) {
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

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }

    private IllegalStateException openSearchFailure(
            String message,
            IOException cause
    ) {
        log.error(message, cause);
        return new IllegalStateException(
                "Product search is temporarily unavailable.",
                cause
        );
    }
}