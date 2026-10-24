
package dev.jahlopez.productsearch.exception;

import dev.jahlopez.productsearch.controller.ProductSearchController;
import dev.jahlopez.productsearch.service.ProductSearchService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProductSearchController.class)
class GlobalExceptionHandlerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ProductSearchService productSearchService;

    @Test
    @DisplayName("Should return 400 Bad Request when validation fails")
    void shouldHandleValidationException() throws Exception {
        mockMvc.perform(post("/api/v1/products/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"minPrice\": -10.00}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").exists())
                .andExpect(jsonPath("$.path")
                        .value("/api/v1/products/search"));
    }

    @Test
    @DisplayName("Should return 404 Not Found when product ID is missing")
    void shouldHandleResourceNotFoundException() throws Exception {
        when(productSearchService.getProductById("non-existent"))
                .thenThrow(new ResourceNotFoundException(
                        "Product not found with ID: non-existent"));

        mockMvc.perform(get("/api/v1/products/search/non-existent"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("Not Found"))
                .andExpect(jsonPath("$.message")
                        .value("Product not found with ID: non-existent"))
                .andExpect(jsonPath("$.path")
                        .value("/api/v1/products/search/non-existent"));
    }
}
