package org.bgcatlas.analysis.db;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Entity class for storing analysis results in the database.
 */
@Entity
@Table(name = "analysis_results")
public class AnalysisResultEntity {
    
    @Id
    private String id;
    
    @Column(name = "dataset_id", nullable = false)
    private String datasetId;
    
    @Column(name = "dataset_name", nullable = false)
    private String datasetName;
    
    @Column(name = "analysis_time", nullable = false)
    private LocalDateTime analysisTime;
    
    @Column(name = "successful", nullable = false)
    private boolean successful;
    
    @Column(name = "error_message")
    private String errorMessage;
    
    @Column(name = "metrics_json", columnDefinition = "TEXT")
    private String metricsJson;
    
    /**
     * Default constructor required by Hibernate.
     */
    public AnalysisResultEntity() {
    }
    
    /**
     * Gets the ID of the analysis result.
     *
     * @return The ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Sets the ID of the analysis result.
     *
     * @param id The ID
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * Gets the ID of the dataset.
     *
     * @return The dataset ID
     */
    public String getDatasetId() {
        return datasetId;
    }
    
    /**
     * Sets the ID of the dataset.
     *
     * @param datasetId The dataset ID
     */
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    /**
     * Gets the name of the dataset.
     *
     * @return The dataset name
     */
    public String getDatasetName() {
        return datasetName;
    }
    
    /**
     * Sets the name of the dataset.
     *
     * @param datasetName The dataset name
     */
    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
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
     * Sets the time when the analysis was performed.
     *
     * @param analysisTime The analysis time
     */
    public void setAnalysisTime(LocalDateTime analysisTime) {
        this.analysisTime = analysisTime;
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
     * Sets whether the analysis was successful.
     *
     * @param successful true if the analysis was successful, false otherwise
     */
    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }
    
    /**
     * Gets the error message if the analysis failed.
     *
     * @return The error message, or null if the analysis was successful
     */
    public String getErrorMessage() {
        return errorMessage;
    }
    
    /**
     * Sets the error message if the analysis failed.
     *
     * @param errorMessage The error message
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    /**
     * Gets the metrics as a JSON string.
     *
     * @return The metrics JSON
     */
    public String getMetricsJson() {
        return metricsJson;
    }
    
    /**
     * Sets the metrics as a JSON string.
     *
     * @param metricsJson The metrics JSON
     */
    public void setMetricsJson(String metricsJson) {
        this.metricsJson = metricsJson;
    }
}