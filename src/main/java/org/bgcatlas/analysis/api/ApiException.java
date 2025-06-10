package org.bgcatlas.analysis.api;

/**
 * Exception thrown when an error occurs during an API call to a public repository.
 */
public class ApiException extends Exception {
    
    private final int statusCode;
    
    /**
     * Creates a new API exception with a message.
     *
     * @param message The error message
     */
    public ApiException(String message) {
        super(message);
        this.statusCode = 0;
    }
    
    /**
     * Creates a new API exception with a message and a cause.
     *
     * @param message The error message
     * @param cause The cause of the exception
     */
    public ApiException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = 0;
    }
    
    /**
     * Creates a new API exception with a message and a status code.
     *
     * @param message The error message
     * @param statusCode The HTTP status code associated with the error
     */
    public ApiException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }
    
    /**
     * Creates a new API exception with a message, a cause, and a status code.
     *
     * @param message The error message
     * @param cause The cause of the exception
     * @param statusCode The HTTP status code associated with the error
     */
    public ApiException(String message, Throwable cause, int statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }
    
    /**
     * Gets the HTTP status code associated with the error.
     *
     * @return The status code
     */
    public int getStatusCode() {
        return statusCode;
    }
}