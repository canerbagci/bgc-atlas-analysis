package org.bgcatlas.analysis.dataset;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the result of analyzing a metagenomic assembly dataset.
 */
public class AnalysisResult {
    
    private final String datasetId;
    private final String datasetName;
    private final LocalDateTime analysisTime;
    private final Map<String, Object> metrics;
    private final boolean successful;
    private final String errorMessage;
    
    /**
     * Creates a new successful analysis result.
     *
     * @param datasetId The ID of the analyzed dataset
     * @param datasetName The name of the analyzed dataset
     * @param metrics The metrics calculated during the analysis
     */
    public AnalysisResult(String datasetId, String datasetName, Map<String, Object> metrics) {
        this.datasetId = datasetId;
        this.datasetName = datasetName;
        this.analysisTime = LocalDateTime.now();
        this.metrics = new HashMap<>(metrics);
        this.successful = true;
        this.errorMessage = null;
    }
    
    /**
     * Creates a new failed analysis result.
     *
     * @param datasetId The ID of the dataset that failed to analyze
     * @param datasetName The name of the dataset that failed to analyze
     * @param errorMessage The error message describing the failure
     */
    public AnalysisResult(String datasetId, String datasetName, String errorMessage) {
        this.datasetId = datasetId;
        this.datasetName = datasetName;
        this.analysisTime = LocalDateTime.now();
        this.metrics = Collections.emptyMap();
        this.successful = false;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Gets the ID of the analyzed dataset.
     *
     * @return The dataset ID
     */
    public String getDatasetId() {
        return datasetId;
    }
    
    /**
     * Gets the name of the analyzed dataset.
     *
     * @return The dataset name
     */
    public String getDatasetName() {
        return datasetName;
    }
    
    /**
     * Gets the time when the analysis was performed.
     *
     * @return The analysis time
     */
    public LocalDateTime getAnalysisTime() {
        return analysisTime;
    }
    
    /**
     * Gets the metrics calculated during the analysis.
     *
     * @return The metrics as a map
     */
    public Map<String, Object> getMetrics() {
        return Collections.unmodifiableMap(metrics);
    }
    
    /**
     * Checks if the analysis was successful.
     *
     * @return true if the analysis was successful, false otherwise
     */
    public boolean isSuccessful() {
        return successful;
    }
    
    /**
     * Gets the error message if the analysis failed.
     *
     * @return The error message, or null if the analysis was successful
     */
    public String getErrorMessage() {
        return errorMessage;
    }
    
    @Override
    public String toString() {
        return "AnalysisResult{" +
                "datasetId='" + datasetId + '\'' +
                ", datasetName='" + datasetName + '\'' +
                ", analysisTime=" + analysisTime +
                ", successful=" + successful +
                ", metrics=" + metrics +
                (errorMessage != null ? ", errorMessage='" + errorMessage + '\'' : "") +
                '}';
    }
}