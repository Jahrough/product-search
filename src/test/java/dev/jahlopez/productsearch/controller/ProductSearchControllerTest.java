package dev.jahlopez.productsearch.controller;

import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import dev.jahlopez.productsearch.exception.GlobalExceptionHandler;
import dev.jahlopez.productsearch.exception.ResourceNotFoundException;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.web.PageableHandlerMethodArgumentResolver;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import tools.jackson.databind.json.JsonMapper;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@DisplayName("Product Search Controller Unit Tests")
class ProductSearchControllerTest {

    private MockMvc mockMvc;

    private JsonMapper objectMapper;

    @Mock
    private ProductSearchService productSearchService;

    @InjectMocks
    private ProductSearchController productSearchController;

    @BeforeEach
    void setUp() {
        // Jackson 3 supports java.time natively and writes dates as ISO strings by default,
        // so no JavaTimeModule or SerializationFeature is needed.
        objectMapper = JsonMapper.builder().build();

        var converter = new JacksonJsonHttpMessageConverter(objectMapper);

        mockMvc = MockMvcBuilders.standaloneSetup(productSearchController)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setCustomArgumentResolvers(new PageableHandlerMethodArgumentResolver())
                .setMessageConverters(converter)
                .build();
    }

    @Nested
    @DisplayName("Search endpoints")
    class SearchEndpointTests {

        @Test
        @DisplayName("Should execute product search and return 200 OK with page results")
        void shouldSearchProducts() throws Exception {
            // Arrange
            ProductSearchQueryDto queryDto = ProductSearchQueryDto.builder()
                    .keyword("Keyboard")
                    .build();

            ProductResponseDto responseDto = ProductResponseDto.builder()
                    .id("prod-123")
                    .sku("SKU-KEY-01")
                    .name("Mechanical Keyboard")
                    .description("RGB backlit mechanical keyboard")
                    .price(new BigDecimal("129.99"))
                    .stockQuantity(30)
                    .build();

            var pageResult = new PageImpl<>(List.of(responseDto), PageRequest.of(0, 10), 1);

            when(productSearchService.searchProducts(any(ProductSearchQueryDto.class), any())).thenReturn(pageResult);

            // Act & Assert
            mockMvc.perform(post("/api/v1/products/search")
                            .param("page", "0")
                            .param("size", "10")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(queryDto)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.content[0].id").value("prod-123"))
                    .andExpect(jsonPath("$.content[0].sku").value("SKU-KEY-01"))
                    .andExpect(jsonPath("$.content[0].name").value("Mechanical Keyboard"));
        }
    }

    @Nested
    @DisplayName("Lookup endpoints")
    class LookupEndpointTests {

        @Test
        @DisplayName("Should retrieve product by ID and return 200 OK")
        void shouldGetProductById() throws Exception {
            // Arrange
            ProductResponseDto responseDto = ProductResponseDto.builder()
                    .id("prod-123")
                    .sku("SKU-KEY-01")
                    .name("Mechanical Keyboard")
                    .price(new BigDecimal("129.99"))
                    .stockQuantity(30)
                    .build();

            when(productSearchService.getProductById(eq("prod-123"))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/v1/products/search/prod-123"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value("prod-123"))
                    .andExpect(jsonPath("$.sku").value("SKU-KEY-01"));
        }

        @Test
        @DisplayName("Should return 404 Not Found when product ID does not exist")
        void shouldReturnNotFoundWhenIdMissing() throws Exception {
            // Arrange
            when(productSearchService.getProductById(eq("unknown-id")))
                    .thenThrow(new ResourceNotFoundException("Product not found with ID: unknown-id"));

            // Act & Assert
            mockMvc.perform(get("/api/v1/products/search/unknown-id"))
                    .andExpect(status().isNotFound());
        }

        @Test
        @DisplayName("Should retrieve product by SKU and return 200 OK")
        void shouldGetProductBySku() throws Exception {
            // Arrange
            ProductResponseDto responseDto = ProductResponseDto.builder()
                    .id("prod-123")
                    .sku("SKU-KEY-01")
                    .name("Mechanical Keyboard")
                    .price(new BigDecimal("129.99"))
                    .stockQuantity(30)
                    .build();

            when(productSearchService.getProductBySku(eq("SKU-KEY-01"))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/v1/products/search/sku/SKU-KEY-01"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.sku").value("SKU-KEY-01"))
                    .andExpect(jsonPath("$.name").value("Mechanical Keyboard"));
        }
    }
}