package dev.jahlopez.productsearch.exception;

/**
 * Raised when a product event is permanently invalid, for example a missing required field or an
 * unsupported event type.
 *
 * <p>Retrying cannot repair such an event, so the Kafka error handler treats it as non-retryable and
 * routes the record straight to the dead-letter topic. Do not use it for infrastructure failures or
 * general programming errors, and keep messages free of event payload values.
 */
public class InvalidProductEventException extends RuntimeException {

    public InvalidProductEventException(String message) {
        super(message);
    }

    public InvalidProductEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
