package dev.jahlopez.productsearch.exception;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Standard error response payload for API exceptions and error handling.")
public class ErrorResponse {

    @Schema(description = "Timestamp when the error occurred", example = "2026-10-12T20:30:00")
    private LocalDateTime timestamp;

    @Schema(description = "HTTP status code", example = "404")
    private int status;

    @Schema(description = "HTTP error classification description", example = "Not Found")
    private String error;

    @Schema(description = "Detailed human-readable error description", example = "Product not found with id: 1")
    private String message;

    @Schema(description = "API endpoint path where the error occurred", example = "/api/v1/products/1")
    private String path;
}