package org.bgcatlas.analysis.db;

import org.bgcatlas.analysis.dataset.AnalysisResult;

import java.util.List;
import java.util.Optional;

/**
 * Interface for database operations related to metagenomic dataset analysis.
 */
public interface DatabaseService {
    
    /**
     * Saves an analysis result to the database.
     *
     * @param result The analysis result to save
     * @return The ID of the saved result
     * @throws DatabaseException if an error occurs during the operation
     */
    String saveAnalysisResult(AnalysisResult result) throws DatabaseException;
    
    /**
     * Retrieves an analysis result from the database by its ID.
     *
     * @param resultId The ID of the analysis result
     * @return The analysis result, or empty if not found
     * @throws DatabaseException if an error occurs during the operation
     */
    Optional<AnalysisResult> getAnalysisResult(String resultId) throws DatabaseException;
    
    /**
     * Retrieves all analysis results for a specific dataset.
     *
     * @param datasetId The ID of the dataset
     * @return A list of analysis results
     * @throws DatabaseException if an error occurs during the operation
     */
    List<AnalysisResult> getAnalysisResultsByDataset(String datasetId) throws DatabaseException;
    
    /**
     * Deletes an analysis result from the database.
     *
     * @param resultId The ID of the analysis result to delete
     * @return true if the result was deleted, false if it was not found
     * @throws DatabaseException if an error occurs during the operation
     */
    boolean deleteAnalysisResult(String resultId) throws DatabaseException;
    
    /**
     * Checks if the database is available.
     *
     * @return true if the database is available, false otherwise
     */
    boolean isDatabaseAvailable();
    
    /**
     * Closes the database connection.
     *
     * @throws DatabaseException if an error occurs during the operation
     */
    void close() throws DatabaseException;
}