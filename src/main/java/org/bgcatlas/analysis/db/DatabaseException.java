package org.bgcatlas.analysis.db;

/**
 * Exception thrown when an error occurs during database operations.
 */
public class DatabaseException extends Exception {
    
    /**
     * Creates a new database exception with a message.
     *
     * @param message The error message
     */
    public DatabaseException(String message) {
        super(message);
    }
    
    /**
     * Creates a new database exception with a message and a cause.
     *
     * @param message The error message
     * @param cause The cause of the exception
     */
    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}