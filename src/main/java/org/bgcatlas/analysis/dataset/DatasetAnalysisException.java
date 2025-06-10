package org.bgcatlas.analysis.dataset;

/**
 * Exception thrown when an error occurs during dataset analysis.
 */
public class DatasetAnalysisException extends Exception {
    
    /**
     * Creates a new dataset analysis exception with a message.
     *
     * @param message The error message
     */
    public DatasetAnalysisException(String message) {
        super(message);
    }
    
    /**
     * Creates a new dataset analysis exception with a message and a cause.
     *
     * @param message The error message
     * @param cause The cause of the exception
     */
    public DatasetAnalysisException(String message, Throwable cause) {
        super(message, cause);
    }
}