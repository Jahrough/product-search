package dev.jahlopez.productsearch.exception;

/**
 * Raised when a product event contradicts already-applied state, for example the same
 * aggregate version carrying different content or a different event identity.
 *
 * <p>Retrying cannot resolve such a conflict, so the error handler treats it as
 * non-retryable and routes the record straight to the dead-letter topic.
 */
public class ProductEventConflictException extends RuntimeException {

    public ProductEventConflictException(String message) {
        super(message);
    }
}
