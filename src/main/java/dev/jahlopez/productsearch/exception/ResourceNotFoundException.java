package dev.jahlopez.productsearch.exception;

/**
 * Custom unchecked exception thrown when a requested resource (such as a product or entity)
 * cannot be found in the database or persistent storage layer.
 * Handled by {@link GlobalExceptionHandler} to return an HTTP 404 Not Found response.
 */
public class ResourceNotFoundException extends RuntimeException {

    /**
     * Constructs a new ResourceNotFoundException with the specified detail message.
     *
     * @param message the detail message explaining which resource was missing
     */
    public ResourceNotFoundException(String message) {
        super(message);
    }

    /**
     * Constructs a new ResourceNotFoundException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the underlying cause of the exception
     */
    public ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}