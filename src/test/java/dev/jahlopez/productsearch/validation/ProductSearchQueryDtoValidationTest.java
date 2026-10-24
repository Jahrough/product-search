package dev.jahlopez.productsearch.validation;

import dev.jahlopez.productsearch.dto.ProductSearchQueryDto;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test suite validating constraint rules on {@link ProductSearchQueryDto}.
 */
class ProductSearchQueryDtoValidationTest {

    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            validator = factory.getValidator();
        }
    }

    @Test
    @DisplayName("Should pass validation when query DTO is valid or empty")
    void shouldPassWhenQueryDtoIsValid() {
        // Arrange
        ProductSearchQueryDto queryDto = ProductSearchQueryDto.builder()
                .keyword("Keyboard")
                .category("Electronics")
                .minPrice(new BigDecimal("10.00"))
                .maxPrice(new BigDecimal("200.00"))
                .inStockOnly(true)
                .build();

        // Act
        Set<ConstraintViolation<ProductSearchQueryDto>> violations = validator.validate(queryDto);

        // Assert
        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("Should fail validation when minimum price is negative")
    void shouldFailWhenMinPriceIsNegative() {
        // Arrange
        ProductSearchQueryDto queryDto = ProductSearchQueryDto.builder()
                .minPrice(new BigDecimal("-5.00"))
                .build();

        // Act
        Set<ConstraintViolation<ProductSearchQueryDto>> violations = validator.validate(queryDto);

        // Assert
        assertThat(violations).hasSize(1);
        ConstraintViolation<ProductSearchQueryDto> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString()).isEqualTo("minPrice");
        assertThat(violation.getMessage()).contains("must be greater than or equal to 0");
    }

    @Test
    @DisplayName("Should fail validation when maximum price is negative")
    void shouldFailWhenMaxPriceIsNegative() {
        // Arrange
        ProductSearchQueryDto queryDto = ProductSearchQueryDto.builder()
                .maxPrice(new BigDecimal("-0.01"))
                .build();

        // Act
        Set<ConstraintViolation<ProductSearchQueryDto>> violations = validator.validate(queryDto);

        // Assert
        assertThat(violations).hasSize(1);
        ConstraintViolation<ProductSearchQueryDto> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString()).isEqualTo("maxPrice");
        assertThat(violation.getMessage()).contains("must be greater than or equal to 0");
    }
}