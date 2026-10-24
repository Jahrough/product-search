package dev.jahlopez.productsearch.service;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import dev.jahlopez.productsearch.exception.ResourceNotFoundException;
import dev.jahlopez.productsearch.mapper.ProductMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Product Search Service")
class ProductSearchServiceTest {

    private static final String PRODUCT_ID = "prod-123";
    private static final String PRODUCT_SKU = "SKU-001";
    private static final String INVALID_ID = "invalid-id";
    private static final String INVALID_SKU = "INVALID-SKU";
    private static final String PRODUCT_NAME = "Wireless Mouse";

    private static final Pageable PAGEABLE = PageRequest.of(0, 10);

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private ProductMapper productMapper;

    @InjectMocks
    private ProductSearchService productSearchService;

    private ProductDocument sampleDocument;
    private ProductResponseDto sampleResponseDto;

    @BeforeEach
    void setUp() {
        sampleDocument = ProductDocument.builder()
                .id(PRODUCT_ID)
                .sku(PRODUCT_SKU)
                .name(PRODUCT_NAME)
                .description("Ergonomic wireless optical mouse")
                .price(new BigDecimal("49.99"))
                .stockQuantity(100)
                .version(1L)
                .build();

        sampleResponseDto = ProductResponseDto.builder()
                .id(PRODUCT_ID)
                .sku(PRODUCT_SKU)
                .name(PRODUCT_NAME)
                .description("Ergonomic wireless optical mouse")
                .price(new BigDecimal("49.99"))
                .stockQuantity(100)
                .build();
    }

    @Nested
    @DisplayName("Product search")
    class ProductSearchTests {

        @Test
        @DisplayName("Should search by keyword and return mapped results")
        void shouldSearchProductsWithKeyword() throws IOException {
            ProductSearchQueryDto query = ProductSearchQueryDto.builder()
                    .keyword("Mouse")
                    .build();

            mockSearchResponse(List.of(sampleDocument));

            when(productMapper.toResponseDto(sampleDocument))
                    .thenReturn(sampleResponseDto);

            Page<ProductResponseDto> result =
                    productSearchService.searchProducts(query, PAGEABLE);

            assertThat(result).isNotNull();
            assertThat(result.getContent())
                    .containsExactly(sampleResponseDto);
            assertThat(result.getTotalElements()).isEqualTo(1);

            verify(openSearchClient).search(
                    any(Function.class),
                    eq(ProductDocument.class)
            );
            verify(productMapper).toResponseDto(sampleDocument);
        }

        @Test
        @DisplayName("Should return all matching products when keyword is blank")
        void shouldSearchProductsWithoutKeyword() throws IOException {
            ProductSearchQueryDto query = ProductSearchQueryDto.builder()
                    .keyword("   ")
                    .build();

            mockSearchResponse(List.of(sampleDocument));

            when(productMapper.toResponseDto(sampleDocument))
                    .thenReturn(sampleResponseDto);

            Page<ProductResponseDto> result =
                    productSearchService.searchProducts(query, PAGEABLE);

            assertThat(result).isNotNull();
            assertThat(result.getContent())
                    .containsExactly(sampleResponseDto);

            verify(openSearchClient).search(
                    any(Function.class),
                    eq(ProductDocument.class)
            );
            verify(productMapper).toResponseDto(sampleDocument);
        }

        @Test
        @DisplayName("Should return an empty page when no products match")
        void shouldReturnEmptyPageWhenNoProductsMatch() throws IOException {
            ProductSearchQueryDto query = ProductSearchQueryDto.builder()
                    .keyword("Nonexistent")
                    .build();

            mockSearchResponse(List.of());

            Page<ProductResponseDto> result =
                    productSearchService.searchProducts(query, PAGEABLE);

            assertThat(result).isNotNull();
            assertThat(result.getContent()).isEmpty();
            assertThat(result.getTotalElements()).isZero();

            verifyNoInteractions(productMapper);
        }
    }

    @Nested
    @DisplayName("Product lookup")
    class ProductLookupTests {

        @Test
        @DisplayName("Should retrieve a product by ID")
        void shouldGetProductById() throws IOException {
            @SuppressWarnings("unchecked")
            GetResponse<ProductDocument> response =
                    mock(GetResponse.class);

            when(response.found()).thenReturn(true);
            when(response.source()).thenReturn(sampleDocument);

            when(openSearchClient.get(
                    any(Function.class),
                    eq(ProductDocument.class)
            )).thenReturn(response);

            when(productMapper.toResponseDto(sampleDocument))
                    .thenReturn(sampleResponseDto);

            ProductResponseDto result =
                    productSearchService.getProductById(PRODUCT_ID);

            assertThat(result).isNotNull();
            assertThat(result.getId()).isEqualTo(PRODUCT_ID);

            verify(openSearchClient).get(
                    any(Function.class),
                    eq(ProductDocument.class)
            );
            verify(productMapper).toResponseDto(sampleDocument);
        }

        @Test
        @DisplayName("Should throw ResourceNotFoundException for a missing ID")
        void shouldThrowExceptionWhenIdNotFound() throws IOException {
            @SuppressWarnings("unchecked")
            GetResponse<ProductDocument> response =
                    mock(GetResponse.class);

            when(response.found()).thenReturn(false);

            when(openSearchClient.get(
                    any(Function.class),
                    eq(ProductDocument.class)
            )).thenReturn(response);

            assertThatThrownBy(
                    () -> productSearchService.getProductById(INVALID_ID)
            )
                    .isInstanceOf(ResourceNotFoundException.class)
                    .hasMessageContaining(
                            "Product not found with ID: " + INVALID_ID
                    );

            verifyNoInteractions(productMapper);
        }

        @Test
        @DisplayName("Should retrieve a product by SKU")
        void shouldGetProductBySku() throws IOException {
            mockSearchResponse(List.of(sampleDocument));

            when(productMapper.toResponseDto(sampleDocument))
                    .thenReturn(sampleResponseDto);

            ProductResponseDto result =
                    productSearchService.getProductBySku(PRODUCT_SKU);

            assertThat(result).isNotNull();
            assertThat(result.getSku()).isEqualTo(PRODUCT_SKU);

            verify(openSearchClient).search(
                    any(Function.class),
                    eq(ProductDocument.class)
            );
            verify(productMapper).toResponseDto(sampleDocument);
        }

        @Test
        @DisplayName("Should throw ResourceNotFoundException for a missing SKU")
        void shouldThrowExceptionWhenSkuNotFound() throws IOException {
            @SuppressWarnings("unchecked")
            SearchResponse<ProductDocument> response = mock(SearchResponse.class);
            @SuppressWarnings("unchecked")
            HitsMetadata<ProductDocument> hitsMetadata = mock(HitsMetadata.class);

            when(hitsMetadata.hits()).thenReturn(List.of());
            when(response.hits()).thenReturn(hitsMetadata);

            when(openSearchClient.search(
                    any(Function.class),
                    eq(ProductDocument.class)
            )).thenReturn(response);

            assertThatThrownBy(
                    () -> productSearchService.getProductBySku(INVALID_SKU)
            )
                    .isInstanceOf(ResourceNotFoundException.class)
                    .hasMessageContaining(
                            "Product not found with SKU: " + INVALID_SKU
                    );

            verifyNoInteractions(productMapper);
        }
    }

    @Nested
    @DisplayName("Product indexing")
    class ProductIndexingTests {

        @Test
        @DisplayName("Should index a product document")
        void shouldIndexProduct() throws IOException {
            when(openSearchClient.index(any(IndexRequest.class)))
                    .thenReturn(mock(IndexResponse.class));

            productSearchService.indexProduct(sampleDocument);

            verify(openSearchClient).index(any(IndexRequest.class));
        }

        @Test
        @DisplayName("Should delete a product index by ID")
        void shouldDeleteProductIndex() throws IOException {
            when(openSearchClient.delete(any(DeleteRequest.class)))
                    .thenReturn(mock(DeleteResponse.class));

            productSearchService.deleteProductIndex(PRODUCT_ID);

            verify(openSearchClient).delete(any(DeleteRequest.class));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void mockSearchResponse(List<ProductDocument> documents)
            throws IOException {

        SearchResponse<ProductDocument> response =
                mock(SearchResponse.class);

        HitsMetadata<ProductDocument> hitsMetadata =
                mock(HitsMetadata.class);

        List<Hit<ProductDocument>> hits = documents.stream()
                .map(document -> {
                    Hit<ProductDocument> hit = mock(Hit.class);
                    lenient().when(hit.source()).thenReturn(document);
                    return hit;
                })
                .toList();

        lenient().when(hitsMetadata.hits()).thenReturn(hits);
        lenient().when(hitsMetadata.total()).thenReturn(null);
        lenient().when(response.hits()).thenReturn(hitsMetadata);

        lenient().when(openSearchClient.search(
                any(Function.class),
                eq(ProductDocument.class)
        )).thenReturn(response);
    }
}