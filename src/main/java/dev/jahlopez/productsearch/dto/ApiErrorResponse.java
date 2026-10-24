package dev.jahlopez.productsearch.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Standardized enterprise error response payload returned by exception handlers
 * across the product-search microservice.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Standardized enterprise error response payload")
public class ApiErrorResponse {

    @Schema(description = "HTTP status code", example = "400")
    private int status;

    @Schema(description = "Machine-readable enterprise error code", example = "INVALID_REQUEST_PAYLOAD")
    private String errorCode;

    @Schema(description = "Human-readable error description", example = "Validation failed for one or more fields.")
    private String message;

    @Schema(description = "API endpoint path where the error occurred", example = "/api/v1/products/search")
    private String path;

    @Schema(description = "Timestamp when the error occurred", example = "2026-10-12T20:30:00Z")
    private Instant timestamp;

    @Schema(description = "Distributed tracing correlation ID for log investigation", example = "a1b2c3d4-e5f6-7890")
    private String traceId;

    @Schema(description = "Detailed field-level validation errors (if applicable)")
    private List<ValidationError> validationErrors;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @Schema(description = "Specific field validation failure details")
    public static class ValidationError {
        @Schema(description = "Field name that failed validation", example = "minPrice")
        private String field;

        @Schema(description = "Reason for failure", example = "must be greater than or equal to 0")
        private String rejectedMessage;
    }
}