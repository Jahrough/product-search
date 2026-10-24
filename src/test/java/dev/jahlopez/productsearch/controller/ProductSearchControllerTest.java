package dev.jahlopez.productsearch.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import dev.jahlopez.productsearch.exception.GlobalExceptionHandler;
import dev.jahlopez.productsearch.exception.ResourceNotFoundException;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProductSearchController.class)
@Import(GlobalExceptionHandler.class)
@DisplayName("Product Search Controller Tests")
class ProductSearchControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ProductSearchService productSearchService;

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